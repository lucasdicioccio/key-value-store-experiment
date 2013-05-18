

def set_key k, v
  puts "set #{k} #{v}"
end

def get_key k
  puts "get #{k}"
end

args        = ARGV.dup
ExitWait    = args.delete('--exit-wait')
MaxKey      = args.shift.to_i
Iterations  = args.shift.to_i
SetRatio    = args.shift.to_f
SetSize     = (args.shift || 64).to_i


File.open('/dev/urandom') do |f|
  Iterations.times do |t|
    k = rand MaxKey
    if rand() < SetRatio
      val = f.read(SetSize).unpack('H2'*SetSize)
      set_key k, val
    else
      get_key k
    end
  end
end

STDIN.gets if ExitWait
