# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/inputs/jdbc"

class logStash::Inputs::JdbcMysql < LogStash::Inputs::Base

  config_name "jdbc_mysql"

  # Host of the MySQL server
  config :host, :validate => :string, :required => true

  # Port of the MySQL server
  config :port, :validate => :number, :default => 3306

  # Database name
  config :db_name, :validate => :string

  # Database username
  config :user, :validate => :string

  # Statement to execute
  config :statement, :validate => :string, :required => true

  config :parameters, :validate => :hash, :default => {}

  config :schedule, :validate => :string

  public

  def register
    require "jdbc/mysql"
    Jdbc::MySQL.load_driver
    @jdbc_input = LogStash::Inputs::Jdbc.new(
      "jdbc_driver_class" => "com.mysql.jdbc.Driver",
      "jdbc_connection_string" => "jdbc:mysql://#{@host}:#{@port}/#{@db_name}",
      "jdbc_user" => @user,
      "jdbc_password" => @password.nil? ? nil : @password.value,
      "statement" => @statement,
      "parameters" => @parameters,
      "schedule" => @schedule
    )
    @jdbc_input.register
  end

  def run
    @jdbc_input.run
  end

  def teardown
    @jdbc_input.teardown
  end
end
