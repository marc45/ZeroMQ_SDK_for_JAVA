package pfe.middleware;

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

public class MainRouter 
{
	
	// variables used to contact NCEM
    static ZContext contextUsedToContactNCEM;
    static ZMQ.Socket reqSocketUsedToContactNCEM;
    
    // MainRouter properties
    static String routerSocketURL; 
    static String subSocketURL, pubSocketURL;



    static private Map<String, String> table = new HashMap<>();


    public static void main( String[] args ) throws IOException
    {
    	Properties ncem = new Properties(); 
    	InputStream inStream = new FileInputStream("NCEM.properties");
    	ncem.load(inStream);
    	
    	System.out.println( "connecting to NCEM    "  + ncem.getProperty("address.rep") ) ;
    	
    	
    	contextUsedToContactNCEM = new ZContext();
    	reqSocketUsedToContactNCEM = contextUsedToContactNCEM.createSocket(SocketType.REQ);
    	reqSocketUsedToContactNCEM.connect(ncem.getProperty("address.rep"));
//    	reqSocketUsedToContactNCEM.setReceiveTimeOut(2000);
//		reqSocketUsedToContactNCEM.setSendTimeOut(2000);
    	String replyFromTheNCEM = null;
    	
    	//send the Router @IP as the body request
        String request = System.getenv("ROUTER_IP_ADDRESS");
        if (request==null)
			   request = "missing info!";


        // we keep sending request until we get a response from the NcemE
        while (replyFromTheNCEM == null) {
            reqSocketUsedToContactNCEM.sendMore("mainRouter");
            reqSocketUsedToContactNCEM.send(request.getBytes(ZMQ.CHARSET), 0);

            replyFromTheNCEM = reqSocketUsedToContactNCEM.recvStr(0);
        }
    	System.out.println("received message from NCEM : [ "+replyFromTheNCEM+" ] ");

    	
    	
    	// replyFromTheNCEM contains the addresses which sockets used by the router are binded to
    	// this includes ROUTER socket address, SUB Socket address, PUB socket address 
    	String[] parts =replyFromTheNCEM.split("-");
    	
    	
    	
    	routerSocketURL = parts[0];
    	pubSocketURL = parts[1];
    	subSocketURL = parts[2];

    	String[] p = parts[3].split(";");
        String s1;
        String s2;

        for (int i=0; i<p.length; i++){
            table.put(p[i].split("!")[0],p[i].split("!")[1]);
        }


    	launchClientServerRouter();	
    	launchSubPubRouter();
    	
    }


	private static void launchSubPubRouter() {
		ZContext contextPublisherSubscriber = new ZContext();
    	ZMQ.Socket threadSocket;
    	
    	
    	// sockets used to communicate with the MainRouter
		final ZMQ.Socket frontendMainRouter = contextPublisherSubscriber.createSocket(SocketType.SUB);
		frontendMainRouter.bind(subSocketURL);
		System.out.println(subSocketURL);
		final ZMQ.Socket backendMainRouter  = contextPublisherSubscriber.createSocket(SocketType.PUB);
		backendMainRouter.bind(pubSocketURL); 
		System.out.println(pubSocketURL);
		
		frontendMainRouter.subscribe("".getBytes());
		
		//thread for sending and receiving messages from and to microservices
		 threadSocket = ZThread.fork(contextPublisherSubscriber, new ZThread.IAttachedRunnable() {
            @Override
            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
        		String message;
        		String topic;
        		while (!Thread.currentThread().isInterrupted()) {
    				topic = frontendMainRouter.recvStr(0);
    				message = frontendMainRouter.recvStr(0);
    				System.out.println(topic);
    				System.out.println(message);
    				
    				
    				//send it to node routers
    				backendMainRouter.sendMore(topic);
    				backendMainRouter.send(message);
    				
        		}
            }
        });
		
		
	}


	private static void launchClientServerRouter() {
		//client server mode
    	final ZContext contextClientServer = new ZContext();
    	ZMQ.Socket threadSocketClientServer;
    	threadSocketClientServer = ZThread.fork(contextClientServer, new ZThread.IAttachedRunnable() {
            @Override
            public void run(Object[] objects, ZContext zContext, ZMQ.Socket socket) {
            	
            	ZMQ.Socket routerSocket = contextClientServer.createSocket(SocketType.ROUTER);
            	routerSocket.bind(routerSocketURL);
            	routerSocket.setSendBufferSize(1024*1024);
            	String source="", destination="", message=""; 
            	routerSocket.setReceiveTimeOut(60);
            	routerSocket.setSendTimeOut(60);
            	String idRouterDestination="";
            	
            	while(!Thread.currentThread().isInterrupted()){
            		source = routerSocket.recvStr(0);
            		System.out.println(source);
            		if(source!=null) {
            			source = routerSocket.recvStr(0);
            			System.out.println(source);
                		destination = routerSocket.recvStr(0);
                		System.out.println(destination);
                		message = routerSocket.recvStr(0);
                		System.out.println(message);
                		
                		idRouterDestination=getIdentityForTheRouter(destination);
                		System.out.println(idRouterDestination);
                		routerSocket.sendMore(idRouterDestination);
                		routerSocket.sendMore(source);
                		routerSocket.sendMore(destination);
                		routerSocket.send(message,0);
            		}				
            	}
            }
            private String getIdentityForTheRouter(String destination) {

                if (destination.contains("#")) //it's a server response
                    return destination.split("#")[0]+"router";

                return table.get(destination)+"router";
			}
        });
	}
}

