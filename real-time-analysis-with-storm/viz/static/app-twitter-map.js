/**
 * get data from redis with the help of EventSource async.
 * @author Leo Zhang 
 */

var colors = 
    [ "#0000FF",
      "#0066FF",
      "#3399FF",
      "#66CCFF",
      "#CCFFFF",
      "#CCFFCC",
      "#FFFFCC",
      "#FFFF99",
      "#FFFF66",
      "#FFFF33",
      "#FFFF00"];
      var source = new EventSource('/stream');
      var hashSentence = {};
      var hashSentiment = {};

//update hash (associative array) with incoming word and count
source.onmessage = function (event) {
  county_id = event.data.split("DELIMETER")[0];
  sentence = event.data.split("DELIMETER")[1];
  count = event.data.split("DELIMETER")[2];
  console.log("NEW DATA IS HERE " + event.data);

  hashSentence[county_id]=sentence;
  hashSentiment[county_id]=count;
};

var map = new Datamap({
    element: document.getElementById('container'),

    scope: 'counties',
    setProjection: function(element, options) {
        var projection, path;
        projection = d3.geo.albersUsa()
            .scale(element.offsetWidth)
            .translate([element.offsetWidth / 2, element.offsetHeight / 2]);

        path = d3.geo.path()
            .projection( projection );

        return {path: path, projection: projection};
    },
    fills: {
        defaultFill: 'green'
    },

    data: {
    },

    geographyConfig: {
        dataUrl: '/static/us.json',
        popupTemplate: function(geo, data) {

          var lineOfTweets = "<p>" + hashSentence[geo.id] + "</p>";
          
          console.log(lineOfTweets)

            return ['<div class="hoverinfo"><strong>',
                    'Top tweets in ', countyLookup[geo.id],  lineOfTweets, 
                    '</strong></div>'].join('');
        }
    }
});






var updateViz =  function(){
	var cnt = 0;
for(key in hashSentiment)
{

    console.log("REFRESH: " + key + ":" + hashSentiment[key]);
    var data = {}; 

    if(hashSentiment[key])
    {
      data[key] = colors[Math.round(hashSentiment[key]%11)];
      map.updateChoropleth(data);
      cnt = cnt + 1;
    }
}
console.log("got #" + cnt + " effectively hot tweets!");

hashSentiment = {};
}

// run updateViz at #7000 milliseconds, or 7 second
window.setInterval(updateViz, 7000);
