package org.sdci.Microservices;

import java.io.IOException;

import org.sdci.sdk.communication.*;
import org.sdci.sdk.service.*;

public class Subscriber extends BasicService {
	public static void main( String[] args ) throws IOException, InterruptedException
    {
		new Subscriber("sub");
		
    }
	
	
	 public Subscriber(String id) throws IOException, InterruptedException {
	        
        XAddCommunicationFeature(new ISubscriberService() {
            @Override
            public void XProcessMessage(String message) {
            	System.out.println("received requestMessage in topic \'time\': ["+message+"]");
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
        System.out.println("Subscribe to Topic : \'time\'");

        XSubscribeToTopic("time");
    }
}
