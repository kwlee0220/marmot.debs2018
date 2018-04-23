package debs2018;

import java.io.Serializable;
import java.util.List;

import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;

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
import marmot.optor.rset.GroupedRecordSet;
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
													.addColumn("departure_port", DataType.STRING)
													.addColumn("ts", DataType.LONG)
													.addColumn("arrival_calc", DataType.LONG)
													.addColumn("arrival_port_calc", DataType.STRING)
//													.addColumn("gradient", DataType.BYTE)
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
		Preconditions.checkArgument(input instanceof GroupedRecordSet);
		
		GroupedRecordSet group = (GroupedRecordSet)input;
		String shipId = (String)group.getGroupKey().getValueAt(0);
		byte shipType = (byte)group.getGroupKey().getValueAt(1);
		
		Observable<ShipTrack> tracks = group.fstream()
											.map(r -> toShiptrack(shipId, shipType, r))
											.observe();
		
		Observable<Record> trajs = ShipTrajectoryDetector.detect(shipId, tracks)
														.flatMap(this::tagDestPort);
		return RecordSets.from(SCHEMA, trajs);
	}
	
	private Observable<Record> tagDestPort(ShipTrajectory shipTraj) {
		String departPort = shipTraj.getDeparturePortName();
		ShipTrack first = shipTraj.getFirstTrack();
		String trjId = String.format("%s_%d", first.shipId(), first.timestamp() / 1000);

		
		return findArrivalTrack(shipTraj)
			.map(arrival ->
					Observable.fromIterable(shipTraj.getTrackAll())
							.map(track -> toRecord(trjId, departPort, arrival._1.timestamp(),
													arrival._2.name(), track)))
			.getOrElse(Observable.empty());
	}
	
	private Option<Tuple2<ShipTrack, Port>> findArrivalTrack(ShipTrajectory shipTraj) {
		Point2f loc = shipTraj.getLastTrack().location();
		Option<Port> oport = m_ports.findNearestValidPort(loc);
		if ( oport.isDefined() ) {
			Port port = oport.get();
			
			List<ShipTrack> tracks =shipTraj.getTrackAll();
			for ( int i = tracks.size()-1; i >=0; --i ) {
				ShipTrack track = tracks.get(i);
				if ( !isArrivedAtPort(track, port) ) {
					return Option.some(Tuple.of(tracks.get(i+1), port));
				}
			}
			
			return Option.some(Tuple.of(tracks.get(0), port));
		}
		
		return m_ports.findNearestValidPort(loc , Ports.DEFAULT_MARGIN)
						.map(port -> Tuple.of(shipTraj.getLastTrack(), port));
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
