require 'typhoeus'
require 'base64'
require 'json'

module FluidDB
  class Row
    class << self

      def ensure_tag(tag, description='FluidRow Tag')
        spaces = tag.split('/')
        tag_name = spaces.pop
        space = spaces.join('/')
        ensure_namespace space

        if DB["tags/#{tag}"].get.nil?
          DB["tags/#{space}"].post({ :name => tag_name, :description => description, :indexed => true })
        end
      end

      def ensure_namespace(space, description='FluidRow Namespace')
        spaces = space.split('/')
        spaces.inject('') do |prefix, name|
          if DB["namespaces#{prefix}/#{name}"].get.nil?
            DB["namespaces#{prefix}"].post({ :name => name, :description => description })
          end

          prefix + '/' + name
        end
      end

      # FluidDB::Row.find('has fluiddb/tags/path', 'fluiddb/tags/*', :limit => 3)
      def find(query, *select)
        opts = select.last.is_a?(Hash) ? select.pop : {}
        select.flatten!

        list = fetch_query(query, opts[:limit], opts[:page])
        fetch_fields(list, select)
      end


      # FluidDB::Row.create('test/curzonj/name' => 'Bob Jones')
      def create(fields={})
        about = fields.delete(:about)
        opts = {}
        opts['about'] = about unless about.nil?

        result = DB['objects'].post(opts)
        update(result['id'], fields) unless fields.empty?
        result['id']
      end

      def update(id, fields)
        fields.each do |tag, value|
          set(id, tag, value)
        end

        execute_batch
      end

      # FluidDB::Row.store(id, 'test/curzonj/rating', 4)
      def set(id, tag, value)
        headers = {}
        headers['Content-Type'] = 'application/vnd.fluiddb.value+json'

        request "objects/#{id}/#{tag}", 
                :method => :put,
                :headers => headers,
                :body => value.to_json
      end

      # TODO if you change to EM you can stream data to fluiddb
      #
      # FluidDB::Row.store(id, 'test/curzonj/name', { :bob => :fred }.to_yaml)
      def store(id, tag, value, content_type='application/octet-stream')
        data = value.to_s

        headers = {}
        headers['Content-Type'] = content_type
        headers['Content-Length'] = data.size

        request "objects/#{id}/#{tag}",
                :method => :put,
                :headers => headers,
                :body => data
      end

      def fetch_query(query, limit=nil, page=nil)
        limit ||= 50
        page ||= 0

        list = DB['objects'].get({ :query => query })['ids']
        list.slice(page*limit, limit)
      end

      def fetch_fields(rows, fields)
        dataset = {}

        rows.each do |id|
          dataset[id] = { }

          resolved_fields(id, fields) do |field|
            fetch_object_field(id, dataset[id], field)
          end
        end
        execute_batch

        dataset
      end

      def resolved_fields(id, fields, &block)
        if fields == [ :all ]
          per_object_tags(id, &block)
        else
          fields.each do |field|
            if field.index('/*') == (field.size - 2)
              wildcard_tags(field, &block)
            else
              block.call(field)
            end
          end
        end
      end

      def wildcard_tags(field)
        path = field[/^(.*)\/\*/, 1]
        request 'namespaces/'+path, :params => {:returnTags => true} do |response|
          json = JSON.parse(response.body)
          json['tagNames'].each do |tag|
            yield "#{path}/#{tag}"
          end
        end
      end

      def per_object_tags(id)
        request "objects/#{id}" do |response|
          json = JSON.parse(response.body)
          json['tagPaths'].each do |tag|
            yield tag
          end
        end
      end

      def fetch_object_field(id, result_set, field)
        request "objects/#{id}/#{field}" do |response|
          is_json = response.headers.split("\r\n").include?("Content-Type: application/vnd.fluiddb.value+json")

          result_set[field] = if is_json
            JSON.parse('[' + response.body + ']').first
          else
            response.body
          end
        end
      end

      def hydra
        @hydra ||= begin
          @cache = {}
          hydra = Typhoeus::Hydra.new
          hydra.cache_setter do |request|
            @cache[request.cache_key] = request.response
          end
          hydra
        end
      end

      def request(path, opts={})
        opts[:headers] ||= {}
        opts[:headers]["Authorization"] = "Basic #{Base64.encode64("#{DB.user}:#{DB.password}")}"
        opts[:timeout] ||= 10000
        opts[:cache_timeout] ||= 2

        req = Typhoeus::Request.new('http://' + FluidDB.base_url + path, opts)
        req.on_complete do |response|
          if [ 200, 204 ].include?(response.code)
            yield response if block_given?
          else
            puts "WARN (#{response.code}): #{path} -- #{response.body}"
          end
        end
          
        hydra.queue req
      end

      def execute_batch
        hydra.run
      end
    end
  end
end
