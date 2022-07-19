package com.anonymous.test.server;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.index.recovery.LeafNodeStatusRecorder;
import com.anonymous.test.storage.driver.DiskDriver;
import com.anonymous.test.storage.flush.S3LayoutSchemaName;
import com.anonymous.test.store.SeriesStore;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * @author anonymous
 * @create 2022-06-25 4:13 PM
 **/
public class SimpleSpringbokServer {

    static SeriesStore seriesStore;

    static ObjectMapper objectMapper = new ObjectMapper();

    static void initEmptySeriesStore() {

        seriesStore = SeriesStoreCreator.createEmptySeriesStore(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-1111", false);

        System.out.println("finish init");
    }

    static void initEmptySeriesStoreWithDiskRecovery() {

        DiskDriver diskDriver = new DiskDriver("/home/anonymous/IdeaProjects/springbok/recovery-test");
        LeafNodeStatusRecorder leafNodeStatusRecorder = new LeafNodeStatusRecorder(diskDriver);
        seriesStore = SeriesStoreCreator.createEmptySeriesStoreWithRecoveryAndDiskFlush(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, "flush-test-1111", leafNodeStatusRecorder);

        System.out.println("finish init");
    }

    static void initAndIngestSeriesStore() {
        String dataFile = "/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv";
        seriesStore = SeriesStoreCreator.createAndFillSeriesStore(dataFile, S3LayoutSchemaName.SPATIO_TEMPORAL, "flush-test-1111", false);
        System.out.println("finish init");
    }

    public static void main(String[] arg) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8001), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));
        initEmptySeriesStoreWithDiskRecovery();
        // pass query params through GET()
        server.createContext("/idtemporalquery", new SimpleSpringbokServer.IdTemporalQueryHandler());
        server.createContext("/insertion", new SimpleSpringbokServer.InsertionHandler());
        server.createContext("/spatialtemporalquery", new SimpleSpringbokServer.SpatialTemporalQueryHandler());
        //server.createContext("/asyncinsertion", new SimpleSpringbokServer.AsyncInsertionHandler());
        server.start();
        System.out.println("server is started");
    }

    static class InsertionHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            /*String getString = httpExchange.getRequestURI().getQuery();
            System.out.println("get string: " + getString);*/

            String postString = IOUtils.toString(httpExchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("data size: " + postString.length());

            List<TrajectoryPoint> pointList = objectMapper.readValue(postString, new TypeReference<List<TrajectoryPoint>>() {});
            for (TrajectoryPoint point : pointList) {
                seriesStore.appendSeriesPoint(point);
            }

            String response = "successful";
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

        }
    }

    static class AsyncInsertionHandler implements HttpHandler {

        static DataBuffer dataBuffer = new DataBuffer();

        static {
            System.out.println("init insertion handler for async");
            Thread consumer = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        dataBuffer.consume(seriesStore);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            });
            consumer.start();

        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {

            System.out.println(Thread.currentThread().getName());

            String getString = httpExchange.getRequestURI().getQuery();
            System.out.println("get string: " + getString);

            String postString = IOUtils.toString(httpExchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("data size: " + postString.length());

            List<TrajectoryPoint> pointList = objectMapper.readValue(postString, new TypeReference<List<TrajectoryPoint>>() {});
            dataBuffer.produce(pointList);

            String response = "successful";
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

        }
    }

    static class DataBuffer {
        LinkedList<TrajectoryPoint> bufferList = new LinkedList<>();

        public void produce(List<TrajectoryPoint> list) {
            synchronized (this) {
                bufferList.addAll(list);
                System.out.println("produce: " + list.size() + " points");
                notify();
            }
        }

        public void consume(SeriesStore seriesStore) throws InterruptedException {
            while (true) {
                synchronized (this) {
                    if (bufferList.size() == 0) {
                        wait();
                    }
                    for (TrajectoryPoint point : bufferList) {
                        seriesStore.appendSeriesPoint(point);
                    }
                    System.out.println("consume: " + bufferList.size() + " points");
                    bufferList.clear();
                    notify();
                }
            }
        }
    }

    static class SpatialTemporalQueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            // from get
            String queryString = httpExchange.getRequestURI().getQuery();
            System.out.println("get string: " + queryString);

            // from post
            String postString = IOUtils.toString(httpExchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("post string: " + postString);

            SpatialTemporalRangeQueryPredicate predicate = objectMapper.readValue(postString, SpatialTemporalRangeQueryPredicate.class);
            System.out.println("predicate: " + predicate);

            List<TrajectoryPoint> result = seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            String resultString = objectMapper.writeValueAsString(result);
            os.write(resultString.getBytes());
            os.close();

        }
    }

    static class IdTemporalQueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // from get
            String queryString = exchange.getRequestURI().getQuery();
            System.out.println("get string: " + queryString);

            // from post
            String postString = IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8);
            System.out.println("post string: " + postString);

            IdTemporalQueryPredicate predicate = objectMapper.readValue(postString, IdTemporalQueryPredicate.class);
            System.out.println("predicate: "  + predicate );

            List<TrajectoryPoint> result = seriesStore.idTemporalQueryWithRefinement(predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());

            exchange.sendResponseHeaders(200, 0);
            OutputStream os = exchange.getResponseBody();
            String resultString = objectMapper.writeValueAsString(result);
            os.write(resultString.getBytes());
            os.close();
        }
    }

}
