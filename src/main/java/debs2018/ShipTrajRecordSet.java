package debs2018;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.geotools.referencing.GeodeticCalculator;

import com.vividsolutions.jts.geom.Point;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.MarmotServer;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import marmot.type.Trajectory;
import marmot.type.Trajectory.Sample;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShipTrajRecordSet extends AbstractRecordSet {
	private static final String PORTS = "debs/ports";
	
	private final RecordSet m_input;
	private final List<Port> m_ports;
	private final Record m_inputRecord;
	private final GeodeticCalculator m_gc = new GeodeticCalculator();
	
	private String m_shipId;
	private String m_trjId;
	private String m_departPort;
	private String m_destPort;
	private Option<Iterator<Sample>> m_iter = null;
	
	private static final RecordSchema SCHEMA
								= RecordSchema.builder()
											.addColumn("the_geom", DataType.POINT)
											.addColumn("trj_id", DataType.STRING)
											.addColumn("ship_id", DataType.STRING)
											.addColumn("depart_port", DataType.STRING)
											.addColumn("dest_port", DataType.STRING)
											.build();
	
	public ShipTrajRecordSet(MarmotServer marmot, RecordSet input) {
		m_input = input;
		m_ports = loadPorts(marmot);
		m_inputRecord = DefaultRecord.of(input.getRecordSchema());
		
		m_iter = findNextTrajectory();
	}

	@Override
	public void close() {
		markClosed(m_input::closeQuietly);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return SCHEMA;
	}
	
	@Override
	public boolean next(Record record) {
		assertNotClosed();
		
		while ( true ) {
			if ( m_iter.isEmpty() ) {
				return false;
			}
			if ( m_iter.get().hasNext() ) {
				break;
			}
			
			m_iter = findNextTrajectory();
		}
		
		Sample sample = m_iter.get().next();
		record.set(0, sample.getPoint());
		record.set(1, m_trjId);
		record.set(2, m_shipId);
		record.set(3, m_departPort);
		record.set(4, m_destPort);
		
		return true;
	}
	
	private Option<Iterator<Sample>> findNextTrajectory() {
		while ( m_input.next(m_inputRecord) ) {
			Trajectory trj = (Trajectory)m_inputRecord.get("trajectory");
			Tuple2<Port,Double> t = findClosestPort(trj.getEndPoint(), m_ports);
			
			if ( Double.compare(t._2, t._1.m_radius) <= 0 ) {
				m_trjId = UUID.randomUUID().toString();
				m_shipId = m_inputRecord.getString("ship_id");
				m_departPort = m_inputRecord.getString("depart_port");
				m_destPort = t._1.m_name;
				
				return Option.some(trj.getSampleAll().iterator());
			}
		}
		
		return Option.none();
	}
	
	private static List<Port> loadPorts(MarmotServer marmot) {
		DataSet ports = marmot.getDataSet(PORTS);
		try ( RecordSet rset = ports.read() ) {
			return rset.fstream()
						.map(r -> new Port(r.getString(1), (Point)r.getGeometry(0),
											r.getDouble(2)))
						.toList();
		}
	}
	
	private Tuple2<Port,Double> findClosestPort(Point loc, List<Port> ports) {
		return FStream.of(ports)
					.map(p -> Tuple.of(p, calcDistance(loc, p.m_loc)))
					.max((t1,t2) -> Double.compare(t2._2, t1._2))
					.get();
	}
	
	private double calcDistance(Point pt1, Point pt2) {
		m_gc.setStartingGeographicPoint(pt1.getX(), pt1.getY());
		m_gc.setDestinationGeographicPoint(pt2.getX(), pt2.getY());
		return m_gc.getOrthodromicDistance();
	}
	
	private static class Port {
		private String m_name;
		private Point m_loc;
		private double m_radius;
		
		Port(String name, Point loc, double radius) {
			m_name = name;
			m_loc = loc;
			m_radius = radius;
		}
		
		public static Port fromString(String str) {
			String[] parts = str.split(":");
			
			double x = Double.parseDouble(parts[1]);
			double y = Double.parseDouble(parts[2]);
			double radius = Double.parseDouble(parts[3]);
			return new Port(parts[0], GeoClientUtils.toPoint(x, y), radius);
		}
		
		@Override
		public String toString() {
			return String.format("%s:%f:%f:%f)",
								m_name, m_loc.getX(), m_loc.getY(), m_radius);
		}
	}
}
