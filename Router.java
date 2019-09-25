package pfe.middleware;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.zeromq.SocketType; 
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZThread;

public class Router
{
	// to identify the router we use
	static String routerUniqueIdentifier;
	
    // variables used to contact NCEM
    static ZContext contextUsedToContactNCEM;
    static ZMQ.Socket reqSocketUsedToContactNCEM;
    
	// MainRouter properties
    static String mainRouterSocketURL, mainSubSocketURL, mainPubSocketURL;
    
    // client server router properties
    static String clientServerRouterURL;
    
    // publisher subscriber router properties
	static String pubSubRouterFrontendURL, pubSubRouterBackendURL;
	
	// configuration router properties
	static String confPubSubRouterFrontendURL, confPubSubRouterBackendURL;




	Map<String, List<LinkedList<String>>> links = new HashMap<>();


	Map<String, ArrayList<String>> identifiers = new HashMap<>();


	public Router() throws IOException, InterruptedException {



		Properties file = new Properties();
		InputStream inStream = new FileInputStream("NCEM.properties");
		file.load(inStream);
		//

		// 2. contact the NCEM to get connection info of the router
		// send the NCEM a get request in order to get the "local" router port number
		// for this we create a context and a REQ socket
		contextUsedToContactNCEM = new ZContext();
		reqSocketUsedToContactNCEM = contextUsedToContactNCEM.createSocket(SocketType.REQ);
		System.out.println("connect to NCEM .. " +  file.getProperty("address.rep"));
		reqSocketUsedToContactNCEM.connect(file.getProperty("address.rep"));
//		reqSocketUsedToContactNCEM.setReceiveTimeOut(1000);
		String replyFromTheNCEM = null;
		// we keep sending request until we get a response from the NCEM

		//send the Router @IP as the body request
		String request = System.getenv("ROUTER_IP_ADDRESS");
		if (request==null)
			request = "missing info!";


		// we keep sending request until we get a response from the NcemE
		while (replyFromTheNCEM == null) {
			reqSocketUsedToContactNCEM.sendMore("nodeRouter");
			reqSocketUsedToContactNCEM.send(request.getBytes(ZMQ.CHARSET), 0);

			replyFromTheNCEM = reqSocketUsedToContactNCEM.recvStr(0);
			System.out.println("Received from NCEM .."+ replyFromTheNCEM);

			if (replyFromTheNCEM !=null &&
                    (replyFromTheNCEM.contains("tcp://null") || replyFromTheNCEM.contains("tcp://:"))){
				System.out.println("Missing NCEM info ! .. retry ..");
				Thread.sleep(100);
				replyFromTheNCEM = null;
			}

		}
		System.out.println("received message from NCEM : [ "+replyFromTheNCEM+" ] ");
		// replyFromTheNCEM contains the addresses which sockets used by the router are binded to
		// this includes ROUTER socket address, SUB Socket address, PUB socket address, PUB socket used for sending configuration messages
		String[] parts =replyFromTheNCEM.split("-");

		routerUniqueIdentifier = parts[0];
		mainRouterSocketURL = parts[1];
		mainSubSocketURL = parts[2];
		mainPubSocketURL = parts[3];
		clientServerRouterURL = parts[4];
		confPubSubRouterFrontendURL = parts[5];
		confPubSubRouterBackendURL = parts[6];
		pubSubRouterFrontendURL= parts[7];
		pubSubRouterBackendURL= parts[8];
		System.out.println(routerUniqueIdentifier);



		confPubSubRouterFrontendURL = file.getProperty("address.pub");



		launchClientServerRouter();
		launchPubSubRouter();
		launchConfigRouter();



	}

	public static void main(String[] args ) throws IOException, InterruptedException {
    	new Router();
    }


    
    //this function is used to receive and send configuration messages
	private void launchConfigRouter() {


    	final ZContext contextConfiguration = new ZContext();
    	ZMQ.Socket threadSocket;
    	final ZMQ.Socket frontend = contextConfiguration.createSocket(SocketType.SUB);
		System.out.println("Connecting to NCEM pub socket .. " + confPubSubRouterFrontendURL);
		frontend.connect(confPubSubRouterFrontendURL);
		frontend.subscribe("".getBytes());
		final ZMQ.Socket backend  = contextConfiguration.createSocket(SocketType.PUB);
		backend.bind(confPubSubRouterBackendURL);  
    	threadSocket = ZThread.fork(contextConfiguration, new ZThread.IAttachedRunnable() {
            @Override
            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
        		String message;
        		String topic;
        		while (!Thread.currentThread().isInterrupted()) {
    				topic = frontend.recvStr(0);
    				message = frontend.recvStr(0);
					System.out.println("[conf] received "+ topic + message);

					if (topic.equals("ConfRouter")){
						if (message.contains(":")) {
							identifiers.clear();
							String[] parts = message.split("@");
							for (int i = 0; i < parts.length; i++) {
								String key = parts[i].split(":")[0];
								String value = parts[i].split(":")[1];

								String[] ids = value.split(";");
								ArrayList<String> arrayList = new ArrayList<>();
								for (int j = 0; j < ids.length; j++) {
									arrayList.add(ids[j]);
								}
								identifiers.put(key, arrayList);
							}
						}
						System.out.println("------------------------");
						identifiers.forEach((key, value)->{
							value.forEach(e->{
								System.out.print( " - "+e);
							});
							System.out.println();
						});
						System.out.println("-------------------------");


					}
					else{
						backend.sendMore(topic+"1");
													// accept that configuration will be applied only to microservices with nb of replicats = 1
													// Althought, get the id of IDENTIFIIERS collection
													// TODO consider all cases
						backend.send(message);
					}


        		}
        		frontend.close();
        		backend.close();
        		contextConfiguration.close();	
            }
        });
	}

	
	// this function is used as a router between publishers and subscriber
	private  void launchPubSubRouter() {
	  	ZContext contextPublisherSubscriber = new ZContext();
    	ZMQ.Socket threadSocketPublisherSubscriber;
    	ZMQ.Socket threadSocketMainRouter;
    	
    	
    	// sockets used to communicate with microservices
    	final ZMQ.Socket frontendMicroServices = contextPublisherSubscriber.createSocket(SocketType.SUB);
		frontendMicroServices.bind(pubSubRouterFrontendURL);
		final ZMQ.Socket backendMicroServices  = contextPublisherSubscriber.createSocket(SocketType.PUB);
		backendMicroServices.bind(pubSubRouterBackendURL);        		
		frontendMicroServices.subscribe("".getBytes());
    	
		
		// sockets used to communicate with the MainRouter
		final ZMQ.Socket frontendMainRouter = contextPublisherSubscriber.createSocket(SocketType.SUB);
		frontendMainRouter.connect(mainPubSocketURL);
		final ZMQ.Socket backendMainRouter  = contextPublisherSubscriber.createSocket(SocketType.PUB);
		backendMainRouter.connect(mainSubSocketURL);        		
		frontendMainRouter.subscribe("".getBytes());
		
		
		//thread for sending and receiving messages from and to microservices
		threadSocketPublisherSubscriber = ZThread.fork(contextPublisherSubscriber, new ZThread.IAttachedRunnable() {
            @Override 
            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
        		String message;
        		String topic;
        		
        		while (!Thread.currentThread().isInterrupted()) {

    				topic = frontendMicroServices.recvStr(0);
    				message = frontendMicroServices.recvStr(0);
    				System.out.println(topic);
    				System.out.println(message);
    				
    				
    				//send it to subscribed microservices
    				backendMicroServices.sendMore(topic);
    				backendMicroServices.send(message);
    				
    				//send it to the main router
    				backendMainRouter.sendMore(topic);
    				backendMainRouter.send(message);
        		}
            }
        });
		
		
		//thread for receiving messages from mainRouter
		threadSocketPublisherSubscriber = ZThread.fork(contextPublisherSubscriber, new ZThread.IAttachedRunnable() {
            @Override
            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
        		String message;
        		String topic;
        		while (!Thread.currentThread().isInterrupted()) {
    				topic = frontendMainRouter.recvStr(0);
    				message = frontendMainRouter.recvStr(0);
    				
    				//send it to subscribed microservices
    				backendMicroServices.sendMore(topic);
    				backendMicroServices.send(message);
    				
        		}
            }
        });
	}



	private  void launchClientServerRouter() {
    	ZContext contextClientServer = new ZContext();
    	ZMQ.Socket threadSocketClientServer, threadSocketMainRouter;
    	
    	
    	final ZMQ.Socket routerSocket = contextClientServer.createSocket(SocketType.ROUTER);
    	System.out.println(clientServerRouterURL);
    	routerSocket.bind(clientServerRouterURL);
    	routerSocket.setSendBufferSize(1024*1024);
    	routerSocket.setReceiveTimeOut(60);
    	routerSocket.setSendTimeOut(60);
    	
    	
    	final ZMQ.Socket dealerSocket = contextClientServer.createSocket(SocketType.DEALER);
    	dealerSocket.connect(mainRouterSocketURL);
    	dealerSocket.setIdentity(routerUniqueIdentifier.getBytes());
    	dealerSocket.setReceiveTimeOut(60);
    	dealerSocket.setSendTimeOut(60);

    	
    	threadSocketClientServer = ZThread.fork(contextClientServer, new ZThread.IAttachedRunnable() {
            @Override
            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
            	String source="", destination="", message="";     	
            	while(!Thread.currentThread().isInterrupted()){
            		source = routerSocket.recvStr(0);
            		if(source!=null ) {      
        				destination = routerSocket.recvStr(0);

						try {
							destination = getEffectiveDestination(destination);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						message = routerSocket.recvStr(0);
                		System.out.println("received from the source : from "+source+" to "+destination+" : "+message);
                		if(destination!=null && message!=null) {
                			if(areInTheSameNode(source,destination)) {
                				routerSocket.sendMore(destination);
                				routerSocket.sendMore(source);
                        		routerSocket.send(message,0);
                        		System.out.println("sent to the destination : from "+source+" to "+destination+" : "+message);
                			}
                			else {
                				dealerSocket.sendMore(source);
                				dealerSocket.sendMore(destination);
                				dealerSocket.send(message,0);
                        		System.out.println("sent to the main router : from "+source+" to "+destination+" : "+message);
                			}	
                		}
            		}				
            	}
            }
			//we use this function to verify that the client and the server are in the same node
			private boolean areInTheSameNode(String source, String destination) {
				String[] parts = source.split("#");
		    	String nodeClient = parts[0];
		    	String[] split2 = destination.split("#");
		    	String nodeServer = split2[0];
		    	if(nodeClient.equals(nodeServer)) {
		    		return true;
		    	}
				return false;
			}
        });
    	
    	
    	threadSocketMainRouter = ZThread.fork(contextClientServer, new ZThread.IAttachedRunnable() {
            @Override
            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
            	String sourceFromRouter="", destinationFromRouter="", messageFromRouter="";        	
            	while(!Thread.currentThread().isInterrupted()){      			
        			sourceFromRouter = dealerSocket.recvStr(0);
        			if (sourceFromRouter!=null) {
       				
        				destinationFromRouter = dealerSocket.recvStr(0);
            			messageFromRouter = dealerSocket.recvStr(0);
						try {
							routerSocket.sendMore(getEffectiveDestination(destinationFromRouter));
							routerSocket.sendMore(sourceFromRouter);
							routerSocket.send(messageFromRouter,0);
							System.out.println("sent to its destination : from "+sourceFromRouter+" to "+destinationFromRouter+" : "+messageFromRouter);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
        			}
            	}
            } 
    	});
	}

	public String getEffectiveDestination(String destination) throws InterruptedException {



		System.out.println("---------get destination---------------");
		identifiers.forEach((key, value)->{
			System.out.println("key : "+ key);
			value.forEach(e->{
				System.out.print( " - "+e);
			});
			System.out.println();
		});
		System.out.println();
		System.out.println("-------------------------");


		ArrayList<String>  arrayList = identifiers.get(destination);

		if (arrayList == null) {
//			linkedList = identifiers.get(destination);
//			Thread.sleep(500);

			return destination;
		}

		Random rand = new Random();
		int value = rand.nextInt(arrayList.size());

		return arrayList.get(value);
	}
}

