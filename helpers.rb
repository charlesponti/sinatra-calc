ERROR = "You did not enter an appropriate choice, you wanker."


def check x
  if x.to_i == 0
    puts "0 is not a valid input"
    return
  end
end

#based on user selection run appropriate functionality
def basic_calc
    # ask the user which operation they want to perform
    print "(a)dd, (s)ubtract, (m)ultiply, (d)ivide: "
    operation = gets.chomp.downcase

    if ['a','s','m','d'].include? operation
        print "Type first number:  "
        first = gettr

        print "Type second number:  "
        second = gettr

        case operation
          when "a" then p first + second
          when "s" then p first - second
          when "m" then p first * second
          when "d" then p (first / second).round(2)
        end
        gets
    else
        error
        gets
    end
end

def advanced_calc
  # ask the user which operation they want to perform
  print "(p)ower, (s)quare root: "
  operation = gets.chomp.downcase

  if ['p','s'].include? operation
      print "enter first number "
      first = gettr

      if operation == "s"
        p Math.sqrt(first)
      elsif operation == 'p'
        print "enter second number "
        second = gettr
        p first**second
      end
  else
      error
  end
  gets
end


