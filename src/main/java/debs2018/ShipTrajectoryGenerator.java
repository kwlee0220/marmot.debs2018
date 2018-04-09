package debs2018;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Point;

import io.reactivex.Observable;
import marmot.MarmotServer;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.optor.rset.GroupedRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.rset.RecordSets;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
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
													.addColumn("depart_port", DataType.STRING)
													.addColumn("arrival_port", DataType.STRING)
													.addColumn("ts", DataType.LONG)
													.build();
	
	private Ports m_ports;
	
	@Override
	public void initialize(MarmotServer marmot, RecordSchema inputSchema) {
		m_ports = Ports.load(marmot);
		
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
		ShipTrack first = shipTraj.getFirstTrack();
		String trjId = String.format("%s_%d", first.shipId(), first.timestamp() / 1000);
		String departPort = shipTraj.getDeparturePortName();
		Point2f loc = shipTraj.getLastTrack().location();
		
		return m_ports.findNearestValidPort(loc)
						.map(destPort ->
								Observable.fromIterable(shipTraj.getTrackAll())
											.map(track -> toRecord(trjId, departPort,
																	destPort.name(), track)))
						.getOrElse(Observable.empty());
	}
	
	private Record toRecord(String trajId, String departPort, String destPort, ShipTrack track) {
		Point2f loc = track.location();
		Point pt = GeoClientUtils.toPoint(loc.getX(), loc.getY());
		
		Record record = DefaultRecord.of(SCHEMA);
		record.set("the_geom", pt);
		record.set("traj_id", trajId);
		record.set("depart_port", departPort);
		record.set("arrival_port", destPort);
		record.set("ts", track.timestamp());
		
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
