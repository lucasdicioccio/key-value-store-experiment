

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


File.open('/dev/urandom') do |f|
  Iterations.times do |t|
    k = rand MaxKey
    if rand() < SetRatio
      val = f.read(16).unpack('H2'*16)
      set_key k, val
    else
      get_key k
    end
  end
end

STDIN.gets if ExitWait
