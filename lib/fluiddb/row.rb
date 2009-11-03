require 'typhoeus'
require 'base64'
require 'json'

module FluidDB
  class Row
    class << self
      def find(query, *select)
        opts = select.last.is_a?(Hash) ? select.pop : {}
        select.flatten!
        req = self.new

        list = req.fetch_query(query, opts[:limit], opts[:page])
        req.fetch_fields(list, select)
      end

      def create

      end

      def update

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
      hydra.run

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

    def request(path, opts={})
      opts[:headers] ||= {}
      opts[:headers]["Authorization"] = "Basic #{Base64.b64encode("#{DB.user}:#{DB.password}")}"

      req = Typhoeus::Request.new('http://' + FluidDB.base_url + path, opts)
      req.on_complete do |response|
        if response.code == 200
          yield response
        else
          puts "WARN (#{response.code}): #{path} -- #{response.body}"
        end
      end
        
      hydra.queue req
    end
  end
end
