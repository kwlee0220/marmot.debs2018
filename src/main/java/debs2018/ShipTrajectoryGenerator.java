package debs2018;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;

import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.MarmotServer;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.geo.GeoUtils;
import marmot.optor.rset.KeyedRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.rset.RecordSets;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import plaslab.debs2018.Port;
import plaslab.debs2018.Ports;
import plaslab.debs2018.ShipTrack;
import plaslab.debs2018.gridcell.Point2f;
import plaslab.debs2018.gridcell.train.ShipTrajectory;
import plaslab.debs2018.gridcell.train.ShipTrajectoryDetector;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShipTrajectoryGenerator extends AbstractRecordSetFunction
									implements Serializable {
	private static final long serialVersionUID = 3136715462538083686L;
	private static final Logger s_logger = LoggerFactory.getLogger(ShipTrajectoryGenerator.class);
	
	private static final RecordSchema SCHEMA
										= RecordSchema.builder()
													.addColumn("the_geom", DataType.POINT)
													.addColumn("traj_id", DataType.STRING)
													.addColumn("ts", DataType.LONG)
													.addColumn("departure_port", DataType.STRING)
													.addColumn("arrival_calc", DataType.LONG)
													.addColumn("arrival_port_calc", DataType.STRING)
													.build();
	
	private transient Ports m_ports;
	private transient GeodeticCalculator m_gc;
	
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
		String shipId = (String)group.getKey().getValueAt(0);
		byte shipType = (byte)group.getKey().getValueAt(1);
		
		Observable<ShipTrack> tracks = group.fstream()
											.map(r -> toShiptrack(shipId, shipType, r))
											.observe();
		
		List<RecordSet> rsetList = ShipTrajectoryDetector.detect(shipId, tracks)
														.flatMapMaybe(this::trim)
														.map(this::toRecordSet)
														.toList()
														.blockingGet();
		return RecordSets.concat(rsetList.get(0).getRecordSchema(), rsetList);
	}
	
//	private RecordSet toRecordSetPair(ShipTrajectory traj) {
//		String trajId = String.format("%s_%d",
//									traj.shipId(), traj.getFirstTrack().timestamp());
//		RecordSet forward = toRecordSet(traj, trajId);
//		
//		ShipTrajectory reversed = reverse(traj);
//		RecordSet backward = toRecordSet(reversed, trajId+"_");
//		
//		return forward;
//	}
	
	private RecordSet toRecordSet(ShipTrajectory traj) {
		long suffix = TimeUnit.MILLISECONDS.toMinutes(traj.getFirstTrack().timestamp());
		String trajId = String.format("%s_%d", traj.shipId(), suffix);
		return toRecordSet(traj, trajId);
	}
	
	private RecordSet toRecordSet(ShipTrajectory traj, String trajId) {
		ShipTrack last = traj.getLastTrack();
		long arrivalTs = last.timestamp();
		String arrivalPort = traj.arrivalPortCalc().get();
		FStream<Record> trace
			= FStream.of(traj.getTrackAll())
					.map(track -> toRecord(trajId, traj.leavingPort(), arrivalTs,
											arrivalPort, track));
		
		return RecordSets.from(SCHEMA, trace);
	}
	
	private Maybe<ShipTrajectory> trim(ShipTrajectory traj) {
		Tuple2<Port,Integer> arrival = findArrivalTrack(traj);
		if ( arrival == null ) {
			return Maybe.empty();
		}
		
		ShipTrack last = traj.getTrackAll().get(arrival._2-1);
		List<ShipTrack> trimmeds = traj.getTrackAll().subList(0, arrival._2);
		traj = new ShipTrajectory(traj.shipId(), traj.leavingPort(), trimmeds)
					.arrivalPortCalc(arrival._1.name())
					.arrivalTsCalc(last.timestamp());
		
		int departIdx = findDepartureTrack(traj);
		if ( departIdx > 0 ) {
			trimmeds = traj.getTrackAll()
							.subList(departIdx, traj.getTrackAll().size());
			ShipTrajectory traj2 = new ShipTrajectory(traj.shipId(), traj.leavingPort(), trimmeds);
			traj.arrivalPortCalc().forEach(port -> traj2.arrivalPortCalc(port));
			traj.arrivalTsCalc().forEach(ts -> traj2.arrivalTsCalc(ts));
			traj = traj2;
		}
		
		if ( traj.length() >= 5 ) {
			return Maybe.just(traj);
		}
		else {
//			System.out.println("drop too short trajectory: " + traj);
			return Maybe.empty();
		}
	}
	
	private Tuple2<Port,Integer> findArrivalTrack(ShipTrajectory traj) {
		Point2f loc = traj.getLastTrack().location();
		Option<Port> oport = m_ports.findNearestValidPort(loc);
		if ( oport.isDefined() ) {
			Port port = oport.get();
			
			List<ShipTrack> tracks =traj.getTrackAll();
			for ( int i = tracks.size()-1; i >=0; --i ) {
				ShipTrack track = tracks.get(i);
				if ( !isArrivedAtPort(track, port) ) {
					return Tuple.of(port, i+2);
				}
			}
			
			return Tuple.of(port, 1);
		}
		
		return m_ports.findNearestValidPort(loc , Ports.DEFAULT_MARGIN)
						.map(port -> Tuple.of(port, traj.length()))
						.getOrNull();
	}
	
	private int findDepartureTrack(ShipTrajectory traj) {
		Port departPort = m_ports.getPort(traj.leavingPort());
		List<ShipTrack> tracks = traj.getTrackAll();
		for ( int i =0; i < tracks.size(); ++i ) {
			ShipTrack track = tracks.get(i);
			double dist = calcDistance(departPort.location(), track.location()) / 1000;
			if ( dist > departPort.radius() ) {
				return i-1;
			}
		}
		
		return -1;
	}
	
	private ShipTrajectory reverse(ShipTrajectory traj) {
		ShipTrack first = traj.getFirstTrack();
		ShipTrack last = traj.getLastTrack();

		Port departPort = m_ports.getPort(traj.leavingPort());
		String arrivalPort = traj.arrivalPortCalc().get();
		long arrivalTs = last.timestamp();
		
		List<ShipTrack> tracks = Lists.newArrayList(traj.getTrackAll());
		Collections.reverse(tracks);
		
		List<ShipTrack> trace
			= FStream.of(tracks)
					.map(track ->
						new ShipTrack(track.taskId(), arrivalPort, track.shipId(),
									track.shipType(), track.location(), track.speed(),
									track.course(), track.heading(),
									arrivalTs-track.timestamp(), (byte)-1))
					.toList();
		
		ShipTrajectory reversed = new ShipTrajectory(traj.shipId(), arrivalPort, trace)
										.arrivalPortCalc(departPort.name());
		
		double dist = calcDistance(first.location(), departPort.location()) / 1000;
		if ( Double.compare(dist, departPort.radius() + Ports.DEFAULT_MARGIN) <= 0 ) {
			reversed.arrivalTsCalc(reversed.getLastTrack().timestamp());
		}
		
		return reversed;
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
	
	private static CoordinateTransform s_trans = CoordinateTransform.get("EPSG:4326", "EPSG:3857");
	private List<ShipTrack> computeGradientIndex(List<Tuple2<ShipTrack,Integer>> tracks) {
		Point2f from2f = tracks.get(0)._1.location();
		Point2f to2f = tracks.get(1)._1.location();
		Coordinate from = s_trans.transform(new Coordinate(from2f.getX(), from2f.getY()));
		Coordinate to = s_trans.transform(new Coordinate(to2f.getX(), to2f.getY()));
		byte index = (byte)Math.round(GeoUtils.gradient(from, to) / 45);
		
		ShipTrack tagged = tracks.get(1)._1.gradient(index);
		if ( tracks.get(0)._2 == 0 ) {
			return Lists.newArrayList(tracks.get(0)._1.gradient((byte)-1), tagged);
		}
		else {
			return Lists.newArrayList(tagged);
		}
	}
	
	private Record toRecord(String trajId, String departPort, long arrivalTs,
							String arrivalPort, ShipTrack track) {
		Point2f loc = track.location();
		Point pt = GeoClientUtils.toPoint(loc.getX(), loc.getY());
		
		Record record = DefaultRecord.of(SCHEMA);
		record.set("the_geom", pt);
		record.set("traj_id", trajId);
		record.set("departure_port", departPort);
		record.set("arrival_calc", arrivalTs);
		record.set("arrival_port_calc", arrivalPort);
		record.set("ts", track.timestamp());
		record.set("gradient", track.gradient());
		
		return record;
	}
	
	private ShipTrack toShiptrack(String shipId, byte shipType, Record record) {
		Point geom = (Point)record.getGeometry("the_geom");
		Point2f loc = new Point2f((float)geom.getX(), (float)geom.getY());
		String departPort = record.getString("departure_port");
		float speed = record.getFloat("speed", (float)-1f);
		short course = record.getShort("course", (short)-1);
		short heading = record.getShort("heading", (short)-1);
		long ts = record.getLong("ts", (long)-1);
		
		return new ShipTrack(departPort, shipId, shipType, loc, speed,
							course, heading, ts);
	}
}
