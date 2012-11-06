# Wukong-Hadoop

The Hadoop plugin for Wukong 3.0. This plugin is designed to help test and run Hadoop jobs using Wukong. Its major purpose is to serve as the bridge between old Wukong (2.0) and new.

## Command Line

Little here has changed:

`wu-hadoop path/to/wukong_processor.rb --input=hdfs://foo/bar/input --output=hdfs://foo/bar/output ...`

The `wu-hadoop` executable comes with all of the same easy-to-use hadoop options as before, and the signature has only changed slightly. Pass it the path to your file with `:mapper` and `:reducer` processors defined (using new Wukong syntax), an ``--input` and an `--output` and everything stays the same. Wu-hadoop will launch your new style Wukong processors on you hadoop cluster using wu-local.