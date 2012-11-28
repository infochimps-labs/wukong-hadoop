require 'configliere'
require 'pathname'
require 'rbconfig'
require 'wukong'

module Wukong
  # Wukong-Hadoop is a plugin for Wukong that lets you develop, test,
  # and run map/reduce type workflows both locally and in the context
  # of a Hadoop cluster.
  #
  # It comes with a binary program called <tt>wu-hadoop</tt> which
  # lets you execute Ruby files containing Wukong processors as well
  # as built-in Wukong widgets.
  module Hadoop
  end
end


require 'wukong-hadoop/configuration'
require 'wukong-hadoop/driver'
require 'wukong-hadoop/extensions'
