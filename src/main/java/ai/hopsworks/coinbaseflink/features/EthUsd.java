package ai.hopsworks.coinbaseflink.features;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import com.logicalclocks.hsfs.flink.FeatureStore;
import com.logicalclocks.hsfs.flink.HopsworksConnection;

import ai.hopsworks.coinbaseflink.utils.Ticker;
import ai.hopsworks.coinbaseflink.utils.WSReader;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

public class EthUsd {

  public static final int CHECKPOINTING_INTERVAL_MS = 5000;
  private static final String JOB_NAME = "Coinbase ETH-USD ticker";

  private FeatureStore featureStore;

  public EthUsd() throws Exception {
    HopsworksConnection hopsworksConnection = HopsworksConnection.builder()
        .host("10.87.43.126")
        .port(443)
        .project("flink_v1")
        .apiKeyValue("MSHiV6jSucZ9wscg.oGAUiQv2gEm2ZiEA5lzSa7R2ZrOvav2oqHS8YgSVOjFxCx4DKZU6vaQ8Diix7vEK")
        .build();

    featureStore = hopsworksConnection.getFeatureStore();
  }

  public void stream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Ticker> websocketSource = env.addSource(new WSReader());

    // Setup the sliding window aggregations 5, 10, 60 minutes
    priceSlidingWindow(websocketSource, 5, 1, "eth_usd_price_5_min", 6);
    priceSlidingWindow(websocketSource, 10, 5, "eth_usd_price_10_min", 6);
    priceSlidingWindow(websocketSource, 60, 10, "eth_usd_price_60_min", 6);

    env.execute(JOB_NAME);
    env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
    env.setRestartStrategy(RestartStrategies.noRestart());
  }

  private void priceSlidingWindow(DataStreamSource<Ticker> websocketSource,
                                  int windowSizeMinutes,
                                  int slideSizeMinutes,
                                  String featureGroupName,
                                  int featureGroupVersion) throws Exception {

    WatermarkStrategy<Ticker> customWatermark = WatermarkStrategy
        .<Ticker>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((event, timestamp) -> event.getTimestamp().toEpochMilli());

    DataStream<PriceAgg> websocketStream = websocketSource
        .assignTimestampsAndWatermarks(customWatermark)
        .keyBy(Ticker::getTicker)
        .window(SlidingEventTimeWindows.of(Time.minutes(windowSizeMinutes), Time.minutes(slideSizeMinutes)))
        .aggregate(new PriceAccumulator(), new PriceWindow());

    featureStore
        .getStreamFeatureGroup(featureGroupName, featureGroupVersion)
        .insertStream(websocketStream);
  }
}
