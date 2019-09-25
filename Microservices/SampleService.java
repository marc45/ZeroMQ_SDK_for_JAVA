package org.sdci.Microservices;

import java.io.IOException;

import org.sdci.sdk.communication.*;
import org.sdci.sdk.service.*;



public class SampleService extends BasicService {
    public SampleService() {
        /*XAddCommunicationFeature(new IConfigurableService() {
            @Override
            public void XProcessConfiguration(String configuration) {
                // Process the received configuration
            }
        });*/
        XAddCommunicationFeature(new IClientService() {});
        /*XAddCommunicationFeature(new IServerService() {
            @Override
            public String XProcessRequest(String sender, String request) {
                // As a server, process here the received request and send back a reponse
            	System.out.println("received request [ "+request+" ] from the client: [ "+ sender+" ]"); 
                return null;
            }
        });
        XAddCommunicationFeature(new ISubscriberService() {
            @Override
            public void XProcessMessage(String requestMessage) {
                // As a subscriber, process here the received requestMessage on a topic
            }
        });
        XAddCommunicationFeature(new IPublisherService() {});*/
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
    	String response;
    	while (true) {
    		response = XSendRequest("srv", "hello from client");
    		System.out.println("received response from the server: [ "+ response+" ]"); 		
    	}
        //  XSubscribeToTopic(topic);
        //  XPublishMessage(topic, requestMessage);
    }
}
