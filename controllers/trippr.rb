get('/trippr') { @title = 'Trip Calculator'; erb :'pages/trippr' }

post '/trippr' do
  distance = params[:distance].to_f
  @mpg = params[:mpg].to_f
  price = params[:price].to_f
  @speed = params[:speed].to_f

  if @speed > 60
    diff = 60  - @speed
    @mpg += ( diff * 2 )
  end

  @time = (distance / @speed).round(1)
  @cost = ( (distance / @mpg) * price ).round(2)

  erb :'pages/trippr'
end
