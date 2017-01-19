package com.seigneurin.sender;

import com.codahale.metrics.Counter;

import java.util.Random;

public class DummySender {

    public static void main(String[] args) throws InterruptedException {

        String kafkaServer = args[0];
        String metricsAppId = args[1];

        Metrics.init(kafkaServer, metricsAppId);

        Counter counter = Metrics.registry.counter("my-counter");

        Random random = new Random(System.currentTimeMillis());

        while (true) {
            counter.inc(random.nextInt(10) + 1);
            Thread.sleep(250);
        }

    }

}
