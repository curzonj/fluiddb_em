module FluidDB
  class Memcache
    CACHE_TTL = 300

    class << self
      def duplicate_requests
        @duplicate_requests ||= {}
      end

      # Caches requests in memcache
      def get(uri, params={}, &block)
        uri = HttpClient.with_params(uri, params)

        if (value = CACHE.get(uri))
          return if block.nil?

          if value == 'currently_running'
            if (req = duplicate_requests[uri])
              req.callback do |response|
                HttpClient.handle_json_response(response, &block)
              end
              req
            else
              cache_miss(uri)
            end
          else
            block.call(*value)
          end
        else
          cache_miss(uri, &block)
        end
      end

      def cache_miss(uri)
        CACHE.add(uri, 'currently_running', 10)
        req = request(uri) do |status, json|
          CACHE.set(uri, [status, json], CACHE_TTL)
          duplicate_requests.delete(uri)

          yield(status, json) if block_given?
        end
        duplicate_requests[uri] = req
      end

      protected
      def request(uri, &block)
        FluidDB.http.request(:get, uri, &block)
      end
    end
  end
end
