package org.events;

import com.snowplowanalytics.snowplow.tracker.Snowplow;
import com.snowplowanalytics.snowplow.tracker.Subject;
import com.snowplowanalytics.snowplow.tracker.Tracker;
import com.snowplowanalytics.snowplow.tracker.configuration.EmitterConfiguration;
import com.snowplowanalytics.snowplow.tracker.configuration.NetworkConfiguration;
import com.snowplowanalytics.snowplow.tracker.configuration.TrackerConfiguration;
import com.snowplowanalytics.snowplow.tracker.events.*;
import com.snowplowanalytics.snowplow.tracker.payload.SelfDescribingJson;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class SnowplowEventsGenerator {
    public static void generateEvents() throws InterruptedException{
        String collectorEndpoint = "http://webhook_endpoint";
        // e.g: http://webhooks.fivetran.com/snowplow/random-string-here

        // the application id to attach to events
        String appId = "fivetran-app-event-tracker";
        // the namespace to attach to events
        String namespace = "fivetran-app";

        // The easiest way to build a tracker is with configuration classes
        TrackerConfiguration trackerConfig = new TrackerConfiguration(namespace, appId);
        NetworkConfiguration networkConfig = new NetworkConfiguration(collectorEndpoint);
        EmitterConfiguration emitterConfig = new EmitterConfiguration().batchSize(5); // send batches of 4 events. In production this number should be higher, depending on the size/event volume

        // We need a tracker to turn our events into something a collector can understand
        final Tracker tracker = Snowplow.createTracker(trackerConfig, networkConfig, emitterConfig);

        System.out.println("Sending events to " + collectorEndpoint);
        System.out.println("Using tracker version " + tracker.getTrackerVersion());


        for(int i = 0;i<10;i++){
            createSelfDescribingEvents(tracker);
            createCustomSelfDescribingEvents(tracker);
            createBadEvents(tracker);
        }

        // Will close all threads and force send remaining events
        tracker.close();
        Thread.sleep(5000);

        System.out.println("Tracked events");
    }

    private static void createSelfDescribingEvents(Tracker tracker){

        // This is an example of a custom context entity
        List<SelfDescribingJson> context = singletonList(
                new SelfDescribingJson(
                        "iglu:com.snowplowanalytics.iglu/anything-c/jsonschema/1-0-0",
                        Collections.singletonMap("daf", "dfad")));

        // This is an example of a ScreenView event which will be translated into a SelfDescribing event
        ScreenView screenView = ScreenView.builder()
                .name("namdafe")
                .id("ifdfd")
                .customContext(context)
                .build();


        // This is an example of a Timing event which will be translated into a SelfDescribing event
        Timing timing = Timing.builder()
                .category("category1")
                .label("label1")
                .variable("variable1")
                .timing(14)
                .customContext(context)
                .build();

        // This is an example of a Structured event
        Structured structured = Structured.builder()
                .category("category2")
                .action("action2")
                .label("label2")
                .property("property2")
                .value(12.324)
                .customContext(context)
                .build();

        // This is an example of a eventSubject for adding user data
        Subject eventSubject = new Subject();
        eventSubject.setUserId("satvik@snowplowanalytics.com");
        eventSubject.setLanguage("EN");

        // This is a sample page view event
        // the eventSubject has been included in this event
        PageView pageViewEvent = PageView.builder()
                .pageTitle("Snowplow Analytics")
                .pageUrl("https://www.snowplowanalytics.com")
                .referrer("https://www.google.com")
                .customContext(context)
                .subject(eventSubject)
                .build();

        // EcommerceTransactions will be deprecated soon: we advise using SelfDescribing events instead
        // EcommerceTransactionItems are tracked as part of an EcommerceTransaction event
        // They are processed into separate events during the `track()` call
        EcommerceTransactionItem item = EcommerceTransactionItem.builder()
                .itemId("order_id1")
                .sku("sku1")
                .price(1.01)
                .quantity(21)
                .name("name1")
                .category("category1")
                .currency("EUR")
                .customContext(context)
                .build();

        // EcommerceTransaction event
        EcommerceTransaction ecommerceTransaction = EcommerceTransaction.builder()
                .orderId("order_id11")
                .totalValue(1.0)
                .affiliation("affiliation1")
                .taxValue(2.0)
                .shipping(3.0)
                .city("city")
                .state("state")
                .country("country")
                .currency("EUR")
                .items(item) // EcommerceTransactionItem events are added to a parent EcommerceTransaction here
                .customContext(context)
                .build();

        SelfDescribing selfDescribing1 = SelfDescribing.builder()
                .eventData(new SelfDescribingJson(
                        "iglu:io.snowplow.foundation/conversion/jsonschema/1-0-0",
                        Map.of("name", "email-signup","value","10")))
                .build();


        SelfDescribing selfDescribing3 = SelfDescribing.builder()
                .eventData(new SelfDescribingJson(
                        "iglu:io.snowplow.foundation/conversion/jsonschema/1-0-0",
                        Map.of("name", "This property is only meant to be 255 characters long. When you send a value that is longer, the event will fail validation and end up as a bad event. In this case that is on purpose, as an exercise to understand Snowplow's concept of bad data. In real life, bad data typically means you need to either update your data structure definitions or your tracking code to resolve the issue.")))
                .build();


        tracker.track(screenView);
        tracker.track(timing);
        tracker.track(structured);
        tracker.track(pageViewEvent); // the .track method schedules the event for delivery to Snowplow
        tracker.track(ecommerceTransaction); // This will track two events
        tracker.track(selfDescribing1);
        tracker.track(selfDescribing3);
    }

    // TODO: Unable to send the bad events to the collector
    private static void createBadEvents(Tracker tracker){
        SelfDescribing selfDescribing1 = SelfDescribing.builder()
                .eventData(new SelfDescribingJson(
                        "iglu:io.snowplow.foundation/conversion/jsonschema/1-0-0",
                        "{Schema: something_random, hello: check}"))
                .build();

        tracker.track(selfDescribing1);

        SelfDescribing selfDescribing2 = SelfDescribing.builder()
                .eventData(new SelfDescribingJson(
                        "iglu:io.snowplow.foundation/conversion/jsonschema/1-0-0",
                        "some random string to have some bad event"))
                .build();

        tracker.track(selfDescribing2);

        SelfDescribing selfDescribing3 = SelfDescribing.builder()
                .eventData(new SelfDescribingJson(
                        "iglu:io.snowplow.foundation/conversion/jsonschema/1-0-0",
                        Map.of("\u0000", " key is some binary value")))
                .build();

        tracker.track(selfDescribing3);
    }

    private static void createCustomSelfDescribingEvents(Tracker tracker){

        // This is an example of a custom context entity
        List<SelfDescribingJson> context = singletonList(
                new SelfDescribingJson(
                        "iglu:com.snowplowanalytics.iglu/anything-c/jsonschema/1-0-0",
                        Collections.singletonMap("adfdfasdf", "chafegn")));

        // This is an example of a custom SelfDescribing event based on a schema
        SelfDescribing selfDescribing = SelfDescribing.builder()
                .eventData(new SelfDescribingJson(
                        "iglu:com.snowplowanalytics.iglu/anything-a/jsonschema/1-0-0",
                        Collections.singletonMap("lastevent", "lasteventfsd")
                ))
                .customContext(context)
                .build();


        tracker.track(selfDescribing);
    }
}
