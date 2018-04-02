package debs2018;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Point;

import io.reactivex.Observable;
import io.vavr.control.Option;
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
	
	private Ports m_ports;
	
	private static final RecordSchema SCHEMA
							= RecordSchema.builder()
											.addColumn("the_geom", DataType.POINT)
											.addColumn("depart_port", DataType.STRING)
											.addColumn("dest_port", DataType.STRING)
											.addColumn("ts", DataType.LONG)
											.build();
	
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
		
		Observable<Record> refineds = ShipTrajectoryDetector.detect(shipId, tracks)
														.flatMapIterable(this::refineTracks);
		return RecordSets.from(SCHEMA, refineds);
	}
	
	private List<Record> refineTracks(ShipTrajectory shipTracks) {
		Point2f loc = shipTracks.getLastTrack().getLocation();
		Point pt = GeoClientUtils.toPoint(loc.getX(), loc.getY());
		Option<Port> destPort = m_ports.findNearestValidPort(pt);
		
		List<Record> records = Lists.newArrayListWithExpectedSize(shipTracks.length());
		for ( ShipTrack track: shipTracks.getTrackAll() ) {
			Record record = DefaultRecord.of(SCHEMA);
			
			loc = track.getLocation();
			record.set("the_geom", GeoClientUtils.toPoint(loc.getX(), loc.getY()));
			record.set("depart_port", shipTracks.getDeparturePortName());
			destPort.forEach(port -> record.set("dest_port", port.m_name));
			record.set("ts", track.getTimestamp());
			
			records.add(record);
		}
		
		return records;
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
