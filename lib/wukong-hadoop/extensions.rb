require_relative("hadoop_env_methods")
Wukong::Processor.class_eval{ include Wukong::Hadoop::EnvMethods }
