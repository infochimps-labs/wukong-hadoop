Wukong.processor(:counter, Wukong::Processor::Accumulator) do

  attr_accessor :count
  
  def start record
    self.count = 0
  end
  
  def accumulate record
    self.count += 1
  end

  def finalize
    yield [key, count]
  end
  
end
