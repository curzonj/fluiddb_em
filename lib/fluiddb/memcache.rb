module FluidDB
  module Memcache
    CACHE_TTL = 300

    # Caches requests in memcache
    def get(uri, params={}, &block)
      uri = HttpClient.with_params(uri, params)

      if (value = CACHE.get(uri))
        return if block.nil?

        if value == 'currently_running'
          if (req = current_requests[uri])
            req.callback do |response|
              HttpClient.handle_json_response(response, &block)
            end
            req
          else
            cache_miss(uri)
          end
        else
          log.debug("Retrieved #{uri} from cache")
          block.call(*value)
        end
      else
        cache_miss(uri, &block)
      end
    end

    private
    def cache_miss(uri)
      CACHE.add(uri, 'currently_running', 10)
      req = request(uri) do |status, json|
        if status == 200
          CACHE.set(uri, [status, json], CACHE_TTL)
        else
          CACHE.delete(uri)
        end

        yield(status, json) if block_given?
      end

      # Set the errback after because if the req is already failed
      # then it would get trapped in the list
      req.errback do
        CACHE.delete(uri)
      end
    end

    def current_requests
      @@current_requests ||= {}
    end

    def join
      handler = lambda do
        log.debug "#{current_requests.size} requests remaining"
        yield if current_requests.empty?
      end

      if current_requests.empty?
        yield
      else
        current_requests.values.each do |req|
          req.callback(&handler)
          req.errback(&handler)
        end
      end
    end

    def request(uri, &block)
      log.debug("Requesting #{uri}")
      req = FluidDB.http.request(:get, uri)
      current_requests[uri] = req

      req.errback { current_requests.delete(uri) }
      req.callback do |response|
        current_requests.delete(uri)
        HttpClient.handle_json_response(response, &block)
      end

      req
    end
  end
end
