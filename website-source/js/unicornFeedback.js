	// get event questions for unicorn feedback
	function getfeedbacksfrombackend() {
		console.info("unicornFeedback - in getfeedbacksfrombackend()");
		if (typeof window.ActiveXObject != 'undefined' ) { 
			xmlDocRef = new ActiveXObject("Microsoft.XMLHTTP");
			xmlDocRef.onreadystatechange = buildfeedbacktable ;
		}
		else {
			xmlDocRef = new XMLHttpRequest();
			xmlDocRef.onload = buildfeedbacktable ;
		}
		// get feedback from backend
        var requestPage = _config.api.invokeUrl + "/getallcontents"
		console.info("unicornFeedback - in getfeedbacksfrombackend() Sending Request= " + requestPage);
		xmlDocRef.open( "GET", requestPage, true );
		xmlDocRef.send( null );
	}// end getfeedbacksfrombackend() function

	function predictsentiment() {
		var checkedValue = ""; 
		var inputElements = document.getElementsByClassName('selectedCb');
		for(var i=0; inputElements[i]; ++i){
			  if(inputElements[i].checked){
				   checkedValue += inputElements[i].value + ",";
			  }
		}

		var lastChar = checkedValue.charAt(checkedValue.length -1); 
		if (lastChar == ','){
			checkedValue = checkedValue.slice(0, -1);
		}

		console.info("checked checkbox IDs are: " + checkedValue);

		//predict sentiment and update database in the backend
        var requestPage = _config.api.invokeUrl + "/predictsentiment?Id=" + checkedValue
		console.info("unicornFeedback - in getfeedbacksfrombackend() Sending Request= " + requestPage);
		xmlDocRef.open( "GET", requestPage, true );
		xmlDocRef.send( null );
        window.location.replace("unicornfeedback.html");

	} //end of predictsentiment() function

	function identifygender() {
		var checkedValue = ""; 
		var inputElements = document.getElementsByClassName('selectedCb');
		for(var i=0; inputElements[i]; ++i){
			  if(inputElements[i].checked){
				   checkedValue += inputElements[i].value + ",";
			  }
		}

		var lastChar = checkedValue.charAt(checkedValue.length -1); 
		if (lastChar == ','){
			checkedValue = checkedValue.slice(0, -1);
		}

		console.info("checked checkbox IDs are: " + checkedValue);

		//predict sentiment and update database in the backend
        var requestPage = _config.api.invokeUrl + "/identifygender?Id=" + checkedValue
		console.info("unicornFeedback - in getfeedbacksfrombackend() Sending Request= " + requestPage);
		xmlDocRef.open( "GET", requestPage, true );
		xmlDocRef.send( null );
        window.location.replace("unicornfeedback.html");

	} //end of predictsentiment() function

	function buildfeedbacktable() {
		if ( xmlDocRef.readyState != 4 ) return ;
		var eventData = xmlDocRef.responseText;
		var buttons = "<button type=\"button\" onclick=\"predictsentiment()\">Predict Sentiment</button> ";
		buttons += "<button type=\"button\" onclick=\"identifygender()\">Identify Gender</button> ";
        buttons += "<br><br><button type=\"button\" onclick=\"window.location.replace('enterfeedback.html')\">Enter a New Customer Feedback</button> ";
		
		console.info("unicornFeedback - in buildfeedbacktable eventData = " + eventData);
		mainTable = "<table><col width=2%><col width=20%><col width=60%><col width=9%><col width=9%><tr><th></th><th>Name</th><th><center>Feedback</center></th><th>Sentiment</th><th>Gender</th></tr>";

		try{
			myObj = JSON.parse(eventData);
			for (i = 0; i < myObj.length; i++) {
				name = myObj[i].FirstName + " " + myObj[i].LastName;
				feedback =  (myObj[i].Feedback==null? "" : myObj[i].Feedback) ;
				sentiment = (myObj[i].Sentiment==null? "" : myObj[i].Sentiment);
				gender = (myObj[i].Gender==null? "" : myObj[i].Gender); 
				mainTable +=  "<tr><td><input type=\"checkbox\" class=\"selectedCb\" name=\"predict\" value=\"" + myObj[i].ID + "\"></td><td>" + name + "</td><td><label>" + feedback + "</td><td>"+ sentiment + "</td><td>"+ gender +"</td></tr>";
			}
		}
		catch(err) {
			if (err instanceof TypeError) {
				console.warn("unicornFeedback - in buildfeedbacktable. root cause is new event no S3 file yet");
				console.warn("unicornFeedback - in buildfeedbacktable. ERROR = " + err);
			}
			else{
				console.error("unicornFeedback - in buildquestionstable. ERROR = " + err);
			}
		}
		mainTable +="</table>";

		document.getElementById("feedbackTable").innerHTML = mainTable;
		document.getElementById("buttons").innerHTML = buttons;
	}// end buildfeedbacktable() function

	function submitFeedback(form) {
		console.info("unicornFeedback - in submitFeedback()");
		lastname = form.lname.value;
		firstname = form.fname.value;
		feedback = form.feedback.value;
		console.info(lastname + ", " + firstname + " and her feedback is: " + feedback);

		if (typeof window.ActiveXObject != 'undefined' ) { 
			xmlDocRef = new ActiveXObject("Microsoft.XMLHTTP");
		}
		else {
			xmlDocRef = new XMLHttpRequest();
		}

        var requestPage = _config.api.invokeUrl + "/enterfeedback?FirstName=" + firstname + "&LastName=" + lastname + "&Feedback=" + feedback
		console.info("unicornFeedback - in submitFeedback() Sending Request= " + requestPage);
		xmlDocRef.open( "GET", requestPage, true );
		xmlDocRef.send( null );
		window.location.replace("unicornfeedback.html");
	}
