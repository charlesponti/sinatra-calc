$(document).ready(function(){
  console.log('loaded and ready to rock');

  setInterval(function() {
    $('#sqrt-answer').html('')
    $('#sqrt-answer').html( Math.floor( Math.sqrt($('#user_val').val()) ) );
  }, 100);
});