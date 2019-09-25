package org.sdci.Microservices;

import java.io.IOException;

import org.sdci.sdk.communication.*;
import org.sdci.sdk.service.*;

public class Server extends BasicService {
		public static void main( String[] args ) throws IOException, InterruptedException {
			new Server("srv");
			
	    }
	
	 	public Server(String id) throws IOException, InterruptedException {
	        
	        XAddCommunicationFeature(new IServerService() {
	            @Override
	            public String XProcessRequest(String sender, String request) {
	                // As a server, process here the received request and send back a reponse
	            	System.out.println("received request [ "+request+" ] from the client: [ "+ sender+" ]"); 
	                return "response from server to request [ " + request + " ]";
	            }
	        });
	        this.StartService(id);
	        this.RunService();
	        //this.StopService();
	       
	    }

	    public void StartService(String idMicroService) throws IOException, InterruptedException {
	        XInitialize();
	        // do other service business stuff
	    }
	    public void StopService(){
	        // do other business stuff
	        XTerminate();
	    }
	    public void RunService() {
	        // do business stuff and make use of any of :
	        //  XSendRequest(destination, requestMessage);
	    	
	        //  XSubscribeToTopic(topic);
	        //  XPublishMessage(topic, requestMessage);
	    }
}
