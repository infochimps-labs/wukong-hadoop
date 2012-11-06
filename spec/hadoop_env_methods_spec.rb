require 'spec_helper'

describe Wukong::Hadoop::EnvMethods do

  subject{ Wukong::Processor.new }
  it{ should respond_to(:input_file)              }
  it{ should respond_to(:input_dir)               }
  it{ should respond_to(:map_input_start_offset)  }
  it{ should respond_to(:map_input_length)        }
  it{ should respond_to(:attempt_id)              }
  it{ should respond_to(:curr_task_id)            }
  it{ should respond_to(:script_cmdline_urlenc)   }
  
end
