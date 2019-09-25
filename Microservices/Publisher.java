package org.sdci.Microservices;



import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.sdci.sdk.communication.*;
import org.sdci.sdk.service.*;

public class Publisher extends BasicService {
	public static void main( String[] args ) throws IOException, InterruptedException
    {
		new Publisher("pub");

    }
	
	
	 public Publisher(String id) throws IOException, InterruptedException {
	        
		 XAddCommunicationFeature(new IPublisherService() {});
        
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
    public void RunService() throws InterruptedException {
        // do business stuff and make use of any of :
        //  XSendRequest(destination, requestMessage);
    	
        //XSubscribeToTopic("temperature");
        //
        int id = 0;
        String message;
    	while(true) {

            message = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());

            XPublishMessage("time", ++id+" -  " + message);
            Thread.sleep(10000);
        }
    	
    }
}