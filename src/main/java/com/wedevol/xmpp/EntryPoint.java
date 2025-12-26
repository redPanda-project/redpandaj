package com.wedevol.xmpp;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

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
    /**
     * Hard-coded demo credentials kept to preserve current behavior until a secure
     * configuration
     * concept is implemented.
     */
    private static final String FCM_PROJECT_SENDER_ID = "987525438504"; // NOSONAR (secrets:S6710): legacy demo requires
                                                                        // inline FCM credentials
    private static final String FCM_SERVER_KEY = "AAAA5e0aXCg:APA91bFoUwqHX8StPUDjqXmA0L5_FCTrQhceDSJ_YxEpQZmCVZtCEDV271GYlvkqRMFizc1YspU4A41t5W8ZGHcKX1FTCpp9_yWj6oc7Q0gru0n8QunhTvH5I-pShvakwH2aGkjxbe_d"; // NOSONAR
                                                                                                                                                                                                             // (secrets:S6710):
                                                                                                                                                                                                             // legacy
                                                                                                                                                                                                             // demo
                                                                                                                                                                                                             // requires
                                                                                                                                                                                                             // inline
                                                                                                                                                                                                             // FCM
                                                                                                                                                                                                             // credentials

    public EntryPoint(String projectId, String apiKey, boolean debuggable) {
        super(projectId, apiKey, debuggable);

        try {
            connect();
        } catch (XMPPException | InterruptedException | KeyManagementException | NoSuchAlgorithmException
                | SmackException
                | IOException e) {
            logger.error("Error trying to connect. Error: {}", e.getMessage());
        }

        // String toRegId =
        // "eJ7JgnPaR-m2ng_anT5Kty:APA91bH0q04VtYWhBzXd5N1GPvVFZ-5Op2mQkH8k38JtNFRmi5ibkA9nmjOQUWN9jf9fXcvpK6ZxwM7wAvwgZ5fdg3A0Q6ibv2V31WwAz_aadodFM9xBeCrGCBN0HP40xPDCB52Vpwd_";
        String toRegId = "exYJbkeSRsmgDXPlfDne4f:APA91bEqPu4sth0fQ83-HtJtI7YzYE0Dbede33DfOiSwcxQUPVo_XnUCmw-0MIcLXzuZpl7_td43UyMRQ-t-6JMI-GMFgOEXf2ZNAvZ-Ho62VwmhLu305By4vUCoKZymbBO34yXatp4_";

        // Send a sample downstream message to a device
        int cnt = 0;
        while (cnt < 1) {
            cnt++;
            final String messageId = Util.getUniqueMessageId();
            final Map<String, String> dataPayload = new HashMap<>();

            final Map<String, String> notificationPayload = new HashMap<>();
            notificationPayload.put("title", "New Message");
            notificationPayload.put("body", "Message could not be decrypted...");
            notificationPayload.put("tag", "default");

            dataPayload.put(Util.PAYLOAD_ATTRIBUTE_MESSAGE, "This is the simple sample message");
            dataPayload.put("data", new Random().nextInt(5000) + "");
            final CcsOutMessage message = new CcsOutMessage(toRegId, messageId, dataPayload);
            message.setCollapseKey("default");

            message.setPriority("10");
            message.setNotificationPayload(notificationPayload);
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
        new EntryPoint(FCM_PROJECT_SENDER_ID, FCM_SERVER_KEY, false);
    }
}
