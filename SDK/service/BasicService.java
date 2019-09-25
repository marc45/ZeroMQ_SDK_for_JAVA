package org.sdci.sdk.service;

import org.sdci.sdk.communication.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZThread;

public abstract class BasicService {
    Map<String, ICommunicationFeature> CommunicationFeatures = new HashMap<String, ICommunicationFeature>();
    
    // to identify the microservice we use
    String microServiceUniqueIdentifier;
    
    
    // variables used to contact NCEM
    ZContext contextUsedToContactNCEM;
    ZMQ.Socket reqSocketUsedToContactNCEM;



    // variables used by the client and the server
    ZContext contextClient, contextServer;
	ZMQ.Socket dealerSocketUsedByClient, dealerSocketUsedByServer;
	String routerSocketUsedByRouterURL; 
	
    
	// variables used by the publisher and the subscriber
    ZContext contextPublisher, contextSubscriber;
	ZMQ.Socket subSocketUsedForReceivingMessages, pubSocketUsedForSendingMessages;
	String subSocketUsedByRouterURL, pubSocketUsedByRouterURL;
	
	
	// variables used if the microService is configurable
	ZContext contextConfiguration;
	String confMessage;
	String confPubSocketUsedByRouter;

	
	
    public void XAddCommunicationFeature (ICommunicationFeature feature) {
        if (feature instanceof IClientService) {
            CommunicationFeatures.put(IClientService.KEY, feature);
            return;
        }
        if (feature instanceof IServerService) {
            CommunicationFeatures.put(IServerService.KEY, feature);
            return;
        }
        if (feature instanceof IPublisherService) {
            CommunicationFeatures.put(IPublisherService.KEY, feature);
            return;
        }
        if (feature instanceof ISubscriberService) {
            CommunicationFeatures.put(ISubscriberService.KEY, feature);
            return;
        }
        if (feature instanceof IConfigurableService) {
            CommunicationFeatures.put(IConfigurableService.KEY, feature);
            return;
        }
    }

    public void XInitialize() throws IOException, InterruptedException {
        //TO-DO

        // 1. retrieve the connection info of the NCEM
        // we suppose that the folder in which the file called "NCEM.properties" is located, is known by every microService
    	// for instance, the file is located in the project folder
    	// we read the url of the NCEM which will give the Uservice the "local" router url
    	Properties ncem = new Properties(); 
    	InputStream inStream = new FileInputStream("NCEM.properties");
    	ncem.load(inStream);
    	
    	    	
    	// 2. contact the NCEM to get connection info of the router
    	// send the NCEM a get request in order to get the "local" router port number
    	// for this we create a context and a REQ socket
    	contextUsedToContactNCEM = new ZContext();
    	reqSocketUsedToContactNCEM = contextUsedToContactNCEM.createSocket(SocketType.REQ);
		System.out.println("connecting to .. NCEM ..  "+ ncem.getProperty("address.rep"));
    	reqSocketUsedToContactNCEM.connect(ncem.getProperty("address.rep"));
    	//reqSocketUsedToContactNCEM.setReceiveTimeOut(1000);
    	String replyFromTheNCEM = null;
    	// we keep sending request until we get a response from the NCEM
    	while( replyFromTheNCEM == null){
    		reqSocketUsedToContactNCEM.sendMore(System.getenv("CLIENT_ID"));
    		String id = System.getenv("ID");
    		reqSocketUsedToContactNCEM.send(id,0);
    		replyFromTheNCEM = reqSocketUsedToContactNCEM.recvStr(0);
				if (replyFromTheNCEM !=null && replyFromTheNCEM.contains("tcp://null")){
					System.out.println("Missing NCEM info ! .. retry ..");
					Thread.sleep(500);
					replyFromTheNCEM = null;
				}
    	}
    	
    	// replyFromTheNCEM contains the addresses which sockets used by the router are binded to
    	// this includes ROUTER socket address, SUB Socket address, PUB socket address, PUB socket used for sending configuration messages 
    	String[] parts =replyFromTheNCEM.split("-");
    	this.microServiceUniqueIdentifier = parts[0];
    	microServiceUniqueIdentifier = this.microServiceUniqueIdentifier;
    	
    	
        // 3. Open the necessary sockets
    	
    	
    	if(CommunicationFeatures.containsKey(IClientService.KEY)) { 
    		contextClient = new ZContext();
    		routerSocketUsedByRouterURL = parts[1];
    		dealerSocketUsedByClient = contextClient.createSocket(SocketType.DEALER);
            System.out.println("connected to Router .. "+routerSocketUsedByRouterURL);
    		dealerSocketUsedByClient.connect(routerSocketUsedByRouterURL);
    		dealerSocketUsedByClient.setIdentity(microServiceUniqueIdentifier.getBytes()); 
    		dealerSocketUsedByClient.setSendBufferSize(1024*1024);
    		dealerSocketUsedByClient.setReceiveTimeOut(1000);
    	}
    	if(CommunicationFeatures.containsKey(IServerService.KEY)) { 
    		contextServer = new ZContext();
    		routerSocketUsedByRouterURL = parts[1];
    		System.out.println("connected to Router .. "+routerSocketUsedByRouterURL);
    		dealerSocketUsedByServer = contextServer.createSocket(SocketType.DEALER);
	    	dealerSocketUsedByServer.connect(routerSocketUsedByRouterURL);
	    	dealerSocketUsedByServer.setIdentity(microServiceUniqueIdentifier.getBytes()); 
	    	dealerSocketUsedByServer.setSendBufferSize(1024*1024);
	    	ZMQ.Socket socketUsedForLaunchingThread;
	    	socketUsedForLaunchingThread = ZThread.fork(contextServer, new ZThread.IAttachedRunnable() {
	            @Override
	            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
	            	String destination, source, request, response;
	                while(!Thread.currentThread().isInterrupted()) {
	                	source = dealerSocketUsedByServer.recvStr(0);
	                    request = dealerSocketUsedByServer.recvStr(0);
	                    response =  ((IServerService) CommunicationFeatures.get(IServerService.KEY)).XProcessRequest(source,request);
	                    XSendResponse( source , response);
	                }
	            }
	        });
    	}
    	if(CommunicationFeatures.containsKey(IPublisherService.KEY)) {
    		contextPublisher = new ZContext();
    		subSocketUsedByRouterURL = parts[3]; 
    		pubSocketUsedForSendingMessages = contextPublisher.createSocket(SocketType.PUB);
            System.out.println("connected to Router .. "+subSocketUsedByRouterURL);
            pubSocketUsedForSendingMessages.connect(subSocketUsedByRouterURL);
    	}
    	if(CommunicationFeatures.containsKey(ISubscriberService.KEY)) {
    		contextSubscriber = new ZContext();
    		pubSocketUsedByRouterURL = parts[4];  
    		subSocketUsedForReceivingMessages = contextSubscriber.createSocket(SocketType.SUB);
            System.out.println("connected to Router .. "+pubSocketUsedByRouterURL);
            subSocketUsedForReceivingMessages.connect(pubSocketUsedByRouterURL);
    		ZMQ.Socket socketUsedForLaunchingThread;
    		socketUsedForLaunchingThread = ZThread.fork(contextSubscriber, new ZThread.IAttachedRunnable() {
                @Override
                public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
                	String message, topic;
                	while(!Thread.currentThread().isInterrupted()) {
                		topic = subSocketUsedForReceivingMessages.recvStr(0);
                    	message = subSocketUsedForReceivingMessages.recvStr(0);
	                    ((ISubscriberService) CommunicationFeatures.get(ISubscriberService.KEY)).XProcessMessage(message);
                    }
                }
            });
    	}
    	if(CommunicationFeatures.containsKey(IConfigurableService.KEY)) {
    		contextConfiguration = new ZContext();
    		ZMQ.Socket subSocketUsedForReceivingConfigurationMessages = contextConfiguration.createSocket(SocketType.SUB);
    		confPubSocketUsedByRouter = parts[2];
			System.out.println("[config] connecting to Router .. "+ confPubSocketUsedByRouter);
    	    subSocketUsedForReceivingConfigurationMessages.connect(confPubSocketUsedByRouter);
    	    subSocketUsedForReceivingConfigurationMessages.subscribe("conf"+microServiceUniqueIdentifier);
    	    ZMQ.Socket socketUsedForLaunchingThread;
			String finalMicroServiceUniqueIdentifier = microServiceUniqueIdentifier;
			socketUsedForLaunchingThread = ZThread.fork(contextConfiguration, new ZThread.IAttachedRunnable() {
                @Override
                public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
					String topic;
                	while(!Thread.currentThread().isInterrupted()) {
                		topic = subSocketUsedForReceivingConfigurationMessages.recvStr(0);
						confMessage = subSocketUsedForReceivingConfigurationMessages.recvStr(0);
						System.out.println("[Info] ["+ finalMicroServiceUniqueIdentifier + " ] receiving config msg : " + confMessage);
                        ((IConfigurableService) CommunicationFeatures.get(IConfigurableService.KEY)).XProcessConfiguration(confMessage);
                    }
                }
            });
    	} 	
    }

	public void XTerminate() {
        //TO-DO
        // Graceful termination of the service:
        //  Inform the NCEM of termination ?
		String replyFromTheNCEM = null;
		while( replyFromTheNCEM == null ){
    		reqSocketUsedToContactNCEM.sendMore(microServiceUniqueIdentifier);
    		reqSocketUsedToContactNCEM.send("DELETE",0);
    		replyFromTheNCEM = reqSocketUsedToContactNCEM.recvStr(0);	
    	}
		
		//close opened connections
		if(!contextUsedToContactNCEM.isClosed()) contextUsedToContactNCEM.close();
		if(!contextConfiguration.isClosed()) contextConfiguration.close();
		if(!contextClient.isClosed()) contextClient.close();
		if(!contextServer.isClosed()) contextServer.close();
		if(!contextPublisher.isClosed()) contextPublisher.close();
		if(!contextSubscriber.isClosed()) contextSubscriber.close();
		
        //  Clean local data ?
		
		
    }
    public String XSendRequest (String destination, String request) {
        // Do not allow the use of this method is the user didn't specify the use of IClientService interface
        assert (CommunicationFeatures.containsKey(IClientService.KEY));  
        String response = null;
        
        String sourceOfResponse, destinationOfResponse;

        // TO-DO:
        // 1. Use the requestSocket to send the request to the given destination
        // we keep sending the request until we get a response from the server
        // usually recv() method will wait until it gets a message, but we set the receive timeout of this socket to 1s, so it will wait only 1s.
        // 2. Wait for the response and send it back
        while(response == null){
        	dealerSocketUsedByClient.sendMore(destination);
            dealerSocketUsedByClient.send(request);
            sourceOfResponse = dealerSocketUsedByClient.recvStr();
            response = dealerSocketUsedByClient.recvStr();
        }
        return response;
    }
    
    public void XSendResponse (String destination, String response) {
        // Do not allow the use of this method is the user didn't specify the use of IClientService interface
        assert (CommunicationFeatures.containsKey(IServerService.KEY));

        // TO-DO:
        // 1. Use the responseSocket to send the request to the given destination
        dealerSocketUsedByServer.sendMore(destination);
        dealerSocketUsedByServer.send(response); 
    }
    
    
    public void XSubscribeToTopic (String topic) {
        // Do not allow the use of this method is the user didn't specify the use of ISubscriberService interface
        assert (CommunicationFeatures.containsKey(ISubscriberService.KEY));

        // TO-DO:
        // 1. Use the subSocket to subscribe to the given topic
        subSocketUsedForReceivingMessages.subscribe(topic);
        
    }
    public void XPublishMessage (String topic, String message) {
        // Do not allow the use of this method is the user didn't specify the use of IPublisherService interface
        assert (CommunicationFeatures.containsKey(IPublisherService.KEY));

        // TO-DO:
        // 1. Use the pubSocket to publish the message in the given topic
        pubSocketUsedForSendingMessages.sendMore(topic);
        pubSocketUsedForSendingMessages.send(message);
    }
}
