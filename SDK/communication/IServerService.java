package org.sdci.sdk.communication;

public interface IServerService extends ICommunicationFeature {
    String KEY = "SERVER";
    String XProcessRequest (String sender, String request);
}
