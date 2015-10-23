module Fluent
module MongoUtil
  def self.included(klass)
    klass.instance_eval {
      config_param :user, :string, :default => nil
      config_param :password, :string, :default => nil, :secret => true
    }
  end
end
end
