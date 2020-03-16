package com.wedevol.xmpp;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import im.redpanda.crypt.Base58;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wedevol.xmpp.bean.CcsOutMessage;
import com.wedevol.xmpp.server.CcsClient;
import com.wedevol.xmpp.util.MessageMapper;
import com.wedevol.xmpp.util.Util;

/**
 * Entry Point class for the XMPP Server
 *
 * @author Charz++
 */
public class EntryPoint extends CcsClient {

    protected static final Logger logger = LoggerFactory.getLogger(EntryPoint.class);

    public EntryPoint(String projectId, String apiKey, boolean debuggable) {
        super(projectId, apiKey, debuggable);

        try {
            connect();
        } catch (XMPPException | InterruptedException | KeyManagementException | NoSuchAlgorithmException | SmackException
                | IOException e) {
            logger.error("Error trying to connect. Error: {}", e.getMessage());
        }

        String toRegId = "";


        // Send a sample downstream message to a device
        int cnt = 0;
        while (cnt < 2) {
            cnt++;
            final String messageId = Util.getUniqueMessageId();
            final Map<String, String> dataPayload = new HashMap<String, String>();
            dataPayload.put(Util.PAYLOAD_ATTRIBUTE_MESSAGE, "This is the simple sample message");
            dataPayload.put("data", new Random().nextInt(5000) + "");
            final CcsOutMessage message = new CcsOutMessage(toRegId, messageId, dataPayload);
            final String jsonRequest = MessageMapper.toJsonString(message);
            sendDownstreamMessage(messageId, jsonRequest);

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
//
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        } catch (InterruptedException e) {
            logger.error("An error occurred while latch was waiting. Error: {}", e.getMessage());
        }
    }

    public static void main(String[] args) throws SmackException, IOException {
        final String fcmProjectSenderId = "987525438504";
        final String fcmServerKey = "AAAA5e0aXCg:APA91bFoUwqHX8StPUDjqXmA0L5_FCTrQhceDSJ_YxEpQZmCVZtCEDV271GYlvkqRMFizc1YspU4A41t5W8ZGHcKX1FTCpp9_yWj6oc7Q0gru0n8QunhTvH5I-pShvakwH2aGkjxbe_d";
        new EntryPoint(fcmProjectSenderId, fcmServerKey, false);
    }
}
