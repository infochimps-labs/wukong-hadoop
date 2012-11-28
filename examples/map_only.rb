Wukong.processor(:mapper) do
  
  field :min_length, Integer,  :default => 1
  field :max_length, Integer,  :default => 256
  field :split_on,   Regexp,   :default => /\s+/
  field :remove,     Regexp,   :default => /[^a-zA-Z0-9\']+/
  field :fold_case,  :boolean, :default => false
  
  def process string
    tokenize(string).each do |token|
      yield token if acceptable?(token)
    end
  end

  private

  def tokenize string
    string.split(split_on).map do |token|
      stripped = token.gsub(remove, '')
      fold_case ? stripped.downcase : stripped
    end
  end

  def acceptable? token
    (min_length..max_length).include?(token.length)
  end
  
end
