// the x dygraphs, and the topics they're currently displaying
var g = [];
var NUM_GRAPHS = 4;
var currently_loading_requests = 0;  // cannot really service this with dygraph
                                     // (no on_load_complete callback) :-(

on_add = function(e) {
    console.log('onclick clicked', e, this);
    var idx = $(this).attr('data-dest');
    console.log('idx', idx);
    var sel = $('#topicselect').val();
    console.log(sel);
    g[idx]['topics'] = sel;
    redraw_graph(idx);
};

redraw_graph = function(idx) {
    // redraw the diagram specified by idx, using
    // the start- and endtimes currently selected
    var csvquery = "/api/v1/getcsv?";
    csvquery += "start="+$('#startdate').val() + "T000000";
    csvquery += "&end="+$('#enddate').val() + "T000000";
    csvquery += "&";
    var sel = g[idx]['topics'].map( x => "t="+x );
    csvquery += sel.join('&');
    console.log(csvquery);
    g[idx]['el'].updateOptions({'file':csvquery});
    var idname = '#topicsdiv'+(Number(idx)+1);
    console.log(idname);
    var td=$(idname);
    console.log(td);
    td.text('Topics: '+g[idx]['topics'].join(', '));
};

on_change_date = function(e) {
    console.log('on change date');
    var startd = $('#startdate').val();
    var endd = $('#enddate').val();
    console.log(startd, endd);
    for(i=0; i<NUM_GRAPHS; i++)
    {
	redraw_graph(i);
    }
};

init = function() {
    var e = $('#topicselect')
    $.ajax({
	type: "GET",
	url: "/api/v1/gettopics",
	success: function(topics){
	    // Parse the returned json data
	    console.log('success - data', topics);  // already parsed!
	    $.each(topics, function(i, d) {
		//console.log('Found topic:',i,d);
		e.append('<option value="' + d + '">' + d + '</option>');
	    });
	}
    });

    $('#add_to_1').on('click', on_add);
    $('#add_to_2').on('click', on_add);
    $('#add_to_3').on('click', on_add);
    $('#add_to_4').on('click', on_add);
    $('#update_dates').on('click', on_change_date);
};  

window.onload = function() {
    init();
    
    ['graphdiv1','graphdiv2','graphdiv3','graphdiv4'].forEach(function(s,idx)
    {
	console.log('Populating DIV:', s);
	el = new Dygraph(
	    document.getElementById(s),
	    // csv-file
	    "/api/v1/getcsv?start=2019-01-27T000000&end=2019-01-28T000000&t=sys/temperatures/abgastemperatur",
	    {
		stepPlot: true,
		connectSeparatedPoints: true,
		axes: {
		    'x': {
			valueFormatter: Dygraph.dateString_,
			ticker: Dygraph.dateTicker,
			axisLabelFormatter: function(d, gran) {
			    return d.toISOString().replace('T','\n').replace('.000Z','Z');
			}
		    }
		},
		xValueParser: function(x) { return 1000*parseFloat(x); }
	    }          // options
	);
	g[idx] = { el:el, topics:[] };
    });
};
