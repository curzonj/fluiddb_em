require 'typhoeus'
require 'base64'
require 'json'

module FluidDB
  class Row
    class << self
      def find(query, *select)
        opts = select.last.is_a?(Hash) ? select.pop : {}
        select.flatten!

        list = fetch_query(query, opts[:limit], opts[:page])
        fetch_fields(list, select)
      end

      def create

      end

      def update

      end 

      def fetch_query(query, limit=nil, page=nil)
        limit ||= 50
        page ||= 0

        list = DB['objects'].get({ :query => query })['ids']
        list.slice(page*limit, limit)
      end

      def fetch_fields(rows, fields)
        hydra = Typhoeus::Hydra.new
        dataset = {}

        rows.each do |id|
          dataset[id] = { }

          fields.each do |field|
            path = "objects/#{id}/#{field}"
            req = Typhoeus::Request.new('http://' + FluidDB.base_url + path,
                                        :headers => {"Authorization" => "Basic #{Base64.b64encode("#{DB.user}:#{DB.password}")}"})
            req.on_complete do |response|
              value = if response.headers.split("\r\n").include?("Content-Type: application/vnd.fluiddb.value+json")
                JSON.parse('[' + response.body + ']').first
              else
                response.body
              end
              dataset[id][field] = value
            end

            hydra.queue req
          end
        end

        hydra.run

        dataset
      end
    end
  end
end
