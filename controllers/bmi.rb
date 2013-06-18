get('/bmi') { @title = 'BMI Calculator'; erb :'pages/bmi' }

post '/bmi' do
  weight = params[:weight].to_f
  height = params[:height].to_f

  case params[:metric]
    when 'lbs' then @result = (weight * 703)/(height**2)
    when 'kgs' then @result = (weight/(height**2))*10000
  end

  erb :'pages/bmi'
end