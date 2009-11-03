$LOAD_PATH << File.dirname(__FILE__)

require 'fluiddb/client'
require 'fluiddb/row'

FluidDB.base_url = 'sandbox.fluidinfo.com/'

module FluidDB
  class Client
    attr_reader :uri, :user, :password
  end

  DB = Client.new({:user => "test", :password => "test"})
end
