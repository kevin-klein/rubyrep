# Used as component of a rubyrep config file.
# Defines connection parameters to the postgresql databases.

RR::Initializer::run do |config|
  config.left = {
    :adapter  => 'postgresql',
    :database => 'rr_left',
    :username => 'postgres',
    :password => 'root110120',
    :host     => 'localhost'
  }

  config.right = {
    :adapter  => 'postgresql',
    :database => 'rr_right',
    :username => 'postgres',
    :password => 'root110120',
    :host     => 'localhost'
  }

  config.include_tables(/./)
  config.exclude_tables 'schema_migrations'
  config.exclude_tables 'ar_internal_metadata'

  config.options[:replication_trace] = true

end
