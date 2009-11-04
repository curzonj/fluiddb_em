module FluidDB
  class Object < Base
    # FluidDB::Object.find('has fluiddb/tags/path', 'fluiddb/tags/*', :limit => 3)
    class << self
      def find(query, *tags)
        opts = tags.last.is_a?(Hash) ? tags.pop : {}

        limit = opts[:limit] || 50
        page = opts[:page] || 0

        get '/objects', :query => query do |status, json|
          if status == 200
            list = json['ids'].slice(page*limit, limit)
            list = list.map {|id| new(id, tags) }

            connection.join { yield list }
          else
            yield nil
          end
        end
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
    end

    attr_reader :id, :attributes
    def initialize(id, *tags)
      @id = id
      @attributes = {}

      fetch_fields(tags) if tags
    end

    def [](k)
      attributes[k]
    end

    def []=(k,v)
      attributes[k] = v
    end

    def update(fields)
      fields.each do |tag, value|
        set(tag, value)
      end
    end

    # Object.new(id).set('test/curzonj/rating', 4)
    def set(tag, value)
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
    def store(id, tag, value)
      data = value.to_s

      headers = {}
      headers['Content-Type'] = content_type
      headers['Content-Length'] = data.size

      request "objects/#{id}/#{tag}",
              :method => :put,
              :headers => headers,
              :body => data
    end

    def store(tag, value, content_type='application/octet-stream', &block)
      request( :put, base_path + tag, nil,
               value.to_s, 'Content-Type' => content_type, &block)
    end

    def base_path
      "/objects/#{id}/"
    end
      

    def fetch_fields(fields)
      fields.flatten!

      resolved_fields(fields) do |tag|
        fetch_object_tag(tag)
      end
    end

    def resolved_fields(fields, &block)
      if fields == [ :all ]
        per_object_tags(&block)
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

    # TODO move to namespaces class
    def wildcard_tags(field)
      path = field[/^(.*)\/\*/, 1]
      self.class.get '/namespaces/'+path, :returnTags => true do |status, json|
        if status == 200
          json['tagNames'].each do |tag|
            yield "#{path}/#{tag}"
          end
        end
      end
    end

    def per_object_tags
      get base_path do |status, json|
        if status == 200
          json['tagPaths'].each do |tag|
            yield tag
          end
        end
      end
    end

    def fetch_object_tag(tag)
      get(base_path + tag) do |status, body|
        attributes[tag] = body if status == 200
      end
    end

  end
end
