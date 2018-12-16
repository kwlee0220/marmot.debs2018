package debs2018;

import java.io.Serializable;
import java.util.List;

import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Point;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.MarmotServer;
import marmot.Record;
import marmot.RecordKey;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.optor.rset.KeyedRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import plaslab.debs2018.Port;
import plaslab.debs2018.Ports;
import plaslab.debs2018.ShipTrack;
import plaslab.debs2018.gridcell.Point2f;
import plaslab.debs2018.support.UnitUtils;
import plaslab.debs2018.support.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShipTrajectoryAdjust extends AbstractRecordSetFunction
									implements Serializable {
	private static final long serialVersionUID = 3136715462538083686L;
	private static final Logger s_logger = LoggerFactory.getLogger(ShipTrajectoryAdjust.class);
	
	private static final RecordSchema SCHEMA
										= RecordSchema.builder()
													.addColumn("the_geom", DataType.POINT)
													.addColumn("timestamp", DataType.STRING)
													.addColumn("ship_type", DataType.BYTE)
													.addColumn("speed", DataType.FLOAT)
													.addColumn("arrival_calc", DataType.STRING)
													.build();
	
	private transient Ports m_ports;
	private transient GeodeticCalculator m_gc;
	
	private static class ShipTrackX {
		private ShipTrack m_track;
		private double m_velo;
		private long m_delta = 0L;
		
		ShipTrackX(ShipTrack track, double velo, long delta) {
			m_track = track;
			m_velo = velo;
			m_delta = delta;
		}
		
		@Override
		public String toString() {
			return m_track.toString();
		}
	}
	
	@Override
	public void initialize(MarmotServer marmot, RecordSchema inputSchema) {
		m_ports = Ports.newInstance();
		m_gc = new GeodeticCalculator();
		setInitialized(marmot, inputSchema, SCHEMA);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		Preconditions.checkArgument(input instanceof KeyedRecordSet);
		
		KeyedRecordSet group = (KeyedRecordSet)input;
		RecordKey key = (RecordKey)group.getKey();
		String shipId = (String)key.getValueAt(0);
		String departPort = (String)key.getValueAt(1);
		String arrivalPort = (String)key.getValueAt(2);
		
		List<ShipTrack> tracks = group.fstream()
									.map(r -> toShiptrack(shipId, departPort, arrivalPort, r))
									.toList();
		return trim(shipId, departPort, arrivalPort, tracks)
				.map(trks -> dropParkingTracks(shipId, departPort, arrivalPort, trks))
				.map(trks -> toRecordSet(departPort, arrivalPort, trks))
				.getOrElse(RecordSet.empty(SCHEMA));
	}
	
	private List<ShipTrack> dropParkingTracks(String shipId, String departPort,
											String arrivalPort, List<ShipTrack> tracks) {
		ShipTrack first = tracks.get(0);
		long dur1 = tracks.get(tracks.size()-1).timestamp() - first.timestamp();
		
		List<ShipTrack> shrinkeds = FStream.of(first)
				.map(track -> new ShipTrackX(track, 100, 0))
				.concatWith(FStream.of(tracks)
									.buffer(2, 1)
									.flatMapOption(pair -> extend(pair)))
				.scan((t1,t2) -> {
					if ( t2.m_velo >= 0.1 ) {
						t2.m_track.timestamp(t1.m_track.timestamp() + t2.m_delta);
						return t2;
					}
					else {
						return t1;
					}
				})
				.map(x -> x.m_track)
				.toList();
		long dur2 = shrinkeds.get(shrinkeds.size()-1).timestamp() - shrinkeds.get(0).timestamp();
		long gap = dur1 - dur2;
		if ( gap > 0 ) {
			System.out.printf("trajectory shrinked: %s:%s->%s: %s->%s (gap=%s)%n",
								shipId, departPort, arrivalPort,
								UnitUtils.toTimeString(dur1),
								UnitUtils.toTimeString(dur2),
								UnitUtils.toTimeString(dur1 - dur2));
		}
		
		return shrinkeds;
	}
	
	private FOption<ShipTrackX> extend(List<ShipTrack> pair) {
		if ( pair.size() == 2 ) {
			ShipTrack prev = pair.get(0);
			ShipTrack track = pair.get(1);
			
			double dist = calcDistance(prev.location(), track.location());
			long elapsed = track.timestamp() - prev.timestamp();
			double velo = (dist * 60 * 60) / elapsed;
			
			return FOption.of(new ShipTrackX(track, velo, elapsed));
		}
		else {
			return FOption.empty();
		}
	}
	
	private Option<List<ShipTrack>> trim(String shipId, String departPort, String arrivalPort,
										List<ShipTrack> tracks) {
		return Option.some(tracks);
/*
		Tuple2<Port,Integer> arrival = findArrivalTrack(tracks);
		if ( arrival == null ) {
			System.out.printf("drop incomplete trajectory: %s:%s->%s (length=%d)%n",
								shipId, departPort, arrivalPort, tracks.size());
			return Option.none();
		}
		
		List<ShipTrack> trimmeds = tracks.subList(0, arrival._2);
		
		int departIdx = findDepartureTrack(departPort, tracks);
		if ( departIdx > 0 ) {
			trimmeds = trimmeds.subList(departIdx, trimmeds.size());
		}
		
		if ( trimmeds.size() >= 5 ) {
			return Option.some(trimmeds);
		}
		else {
//			System.out.printf("drop too short trajectory: %s:%s->%s (length=%d)%n",
//								shipId, departPort, arrivalPort, trimmeds.size());
			return Option.none();
		}
*/
	}
	
	private Tuple2<Port,Integer> findArrivalTrack(List<ShipTrack> tracks) {
		Point2f loc = tracks.get(tracks.size()-1).location();
		Option<Port> oport = m_ports.findNearestValidPort(loc);
		if ( oport.isDefined() ) {
			Port port = oport.get();
			
			for ( int i = tracks.size()-1; i >=0; --i ) {
				ShipTrack track = tracks.get(i);
				if ( !isArrivedAtPort(track, port) ) {
					return Tuple.of(port, i+2);
				}
			}
			
			return Tuple.of(port, 1);
		}
		
		return m_ports.findNearestValidPort(loc , Ports.DEFAULT_MARGIN)
						.map(port -> Tuple.of(port, tracks.size()))
						.getOrNull();
	}
	
	private int findDepartureTrack(String departPort, List<ShipTrack> tracks) {
		Port dp = m_ports.getPort(departPort);
		for ( int i =0; i < tracks.size(); ++i ) {
			ShipTrack track = tracks.get(i);
			double dist = calcDistance(dp.location(), track.location()) / 1000;
			if ( dist > dp.radius() ) {
				return i-1;
			}
		}
		
		return -1;
	}
	
	private boolean isArrivedAtPort(ShipTrack track, Port port) {
		double distInKm = calcDistance(port.location(), track.location()) / 1000;
		return Double.compare(distInKm, port.radius()) <= 0;
	}
	
	private double calcDistance(Point2f pt1, Point2f pt2) {
		m_gc.setStartingGeographicPoint(pt1.getX(), pt1.getY());
		m_gc.setDestinationGeographicPoint(pt2.getX(), pt2.getY());
		return m_gc.getOrthodromicDistance();
	}
	
	private RecordSet toRecordSet(String departPort, String arrivalPort,
								List<ShipTrack> tracks) {
		ShipTrack last = tracks.get(tracks.size()-1);
		
		return RecordSet.from(SCHEMA, FStream.of(tracks)
									.map(track -> toRecord("", departPort, last.timestamp(),
															arrivalPort, track)));
	}
	
	private Record toRecord(String trajId, String departPort, long arrivalTs,
							String arrivalPort, ShipTrack track) {
		Point2f loc = track.location();
		Point pt = GeoClientUtils.toPoint(loc.getX(), loc.getY());
		
		Record record = DefaultRecord.of(SCHEMA);
		record.set("the_geom", pt);
		record.set("traj_id", trajId);
		record.set("ship_type", track.shipType());
		record.set("speed", track.speed());
		record.set("departure_port", departPort);
		record.set("arrival_calc", Utilities.toDateTimeString(arrivalTs));
		record.set("arrival_port_calc", arrivalPort);
		record.set("timestamp", track.timestampAsString());
		record.set("gradient", track.gradient());
		
		return record;
	}
	
	private ShipTrack toShiptrack(String shipId, String departPort, String arrivalPort, Record record) {
		Point geom = (Point)record.getGeometry("the_geom");
		Point2f loc = new Point2f((float)geom.getX(), (float)geom.getY());
		byte shipType = record.getByte("ship_type");
		float speed = record.getFloat("speed");
		short course = record.getShort("course");
		short heading = record.getShort("heading");
		long ts = record.getLong("ts");
		
		return new ShipTrack(departPort, shipId, shipType, loc, speed,
							course, heading, ts)
				.arrivalPort(arrivalPort);
	}
}
