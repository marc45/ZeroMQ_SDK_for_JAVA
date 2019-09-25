package org.sdci.Microservices;

import java.io.IOException;

import org.sdci.sdk.communication.*;
import org.sdci.sdk.service.*;

public class Client extends BasicService  implements IConfigurableService{

    public String requestMessage = " Client request ";


	public static void main( String[] args ) throws IOException, InterruptedException
    {
		new Client("clt");
		
    }
	public Client(String id) throws IOException, InterruptedException {       
        XAddCommunicationFeature(new IClientService() {});
        XAddCommunicationFeature(this);
        this.StartService(id);
        this.RunService();
        this.StopService();
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
    	String response;
    	int i = 0 ; 
    	while (true) {
    		response = XSendRequest("srv", i+ "- "+ requestMessage);
    		System.out.println("Received response from the server: [ "+ response+" ]");
    		Thread.sleep(10000);
    		i++;
    	}
        //  XSubscribeToTopic(topic);
        //  XPublishMessage(topic, requestMessage);
    }

    @Override
    public void XProcessConfiguration(String s) {
        this.requestMessage = s;
    }
}
