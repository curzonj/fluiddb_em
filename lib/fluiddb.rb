$LOAD_PATH << File.dirname(__FILE__)

require 'eventmachine'
require 'base64'
require 'json'
require 'loggable'

require 'fluiddb/http_client'
require 'fluiddb/request_tracking'
require 'fluiddb/memcache'
require 'fluiddb/requestor'
require 'fluiddb/base'
require 'fluiddb/object'

module FluidDB
  class << self
    include Requestor

    attr_reader :http
    def connect(user, password, instance=:sandbox)
      @http = case instance
      when :sandbox
        HttpClient.connect('http://sandbox.fluidinfo.com', user, password)
      when :production
        HttpClient.connect('http://fluiddb.fluidinfo.com', user, password)
      else
        HttpClient.connect(instance, user, password)
      end
    end

  end
end
