package debs2018;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Point;

import io.reactivex.Observable;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.MarmotServer;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.SquareGridCellAssigner;
import marmot.geo.SquareGridCellAssigner.Assignment;
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
	private static final Logger s_logger = LoggerFactory.getLogger(ShipTrajRecordSet.class);
	
	private final RecordSet m_input;
	private final List<Port> m_ports;
	private final Record m_inputRecord;
	private final GeodeticCalculator m_gc = new GeodeticCalculator();
	private final SquareGridCellAssigner m_assigner;
	
	private String m_shipId;
	private String m_trjId;
	private String m_departPort;
	private String m_destPort;
	private Option<Iterator<Tuple2<Sample,Assignment>>> m_iter = null;
	
	private static final RecordSchema SCHEMA
								= RecordSchema.builder()
											.addColumn("the_geom", DataType.POLYGON)
											.addColumn("trj_id", DataType.STRING)
											.addColumn("ship_id", DataType.STRING)
											.addColumn("depart_port", DataType.STRING)
											.addColumn("dest_port", DataType.STRING)
											.addColumn("cell_id", DataType.LONG)
											.addColumn("cell_pos", DataType.GRID_CELL)
											.build();
	
	public ShipTrajRecordSet(MarmotServer marmot, RecordSet input) {
		m_input = input;
		m_ports = loadPorts(marmot);
		m_inputRecord = DefaultRecord.of(input.getRecordSchema());
		
		m_assigner = new SquareGridCellAssigner(Globals.BOUNDS, Globals.RESOLUTION);

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
		
		Tuple2<Sample,Assignment> attached = m_iter.get().next();
		record.set(0, attached._2.getCellPolygon());
		record.set(1, m_trjId);
		record.set(2, m_shipId);
		record.set(3, m_departPort);
		record.set(4, m_destPort);
		record.set(5, attached._2.getCellId());
		record.set(6, attached._2.getCellPos());
		
		return true;
	}
	
	private Option<Iterator<Tuple2<Sample,Assignment>>> findNextTrajectory() {
		while ( m_input.next(m_inputRecord) ) {
			Trajectory traj = (Trajectory)m_inputRecord.get("trajectory");
			Option<Port> destPort = findClosestPort(traj.getEndPoint());
			
			if ( destPort.isDefined() ) {
				m_trjId = UUID.randomUUID().toString();
				m_shipId = m_inputRecord.getString("ship_id");
				m_departPort = m_inputRecord.getString("depart_port");
				m_destPort = destPort.get().m_name;
				
				if ( traj.getDuration().compareTo(Duration.ofHours(1)) < 0
					&& m_departPort.equals(m_destPort) ) {
					continue;
				}

				List<Tuple2<Sample,Assignment>> attacheds = removeDuplicateCells(traj);
				
				return Option.some(attacheds.iterator());
			}
			else {
				if ( s_logger.isInfoEnabled() ) {
					Point pt = traj.getEndPoint();
					
					Tuple2<Port,Double> closest = FStream.of(m_ports)
						.map(port -> Tuple.of(port, calcDistance(pt, port.m_loc)-port.m_radius))
						.min(t -> t._2)
						.get();
					s_logger.info(String.format("fails to find the right port: "
												+ "closest=%s (delta=%.1fkm): %s:%s(%d)",
												closest._1.m_name, closest._2 / 1000,
												m_shipId, m_departPort, traj.getSampleCount()));
				}
			}
		}
		
		return Option.none();
	}
	
	private List<Tuple2<Sample,Assignment>> removeDuplicateCells(Trajectory traj) {
		return Observable
				.fromIterable(traj.getSampleAll())
				.map(s -> Tuple.of(s, m_assigner.assign(s.getPoint())))
				.distinctUntilChanged(t -> t._2.getCellId())
				.toList()
				.blockingGet();
	}
	
	private static List<Port> loadPorts(MarmotServer marmot) {
		return marmot.getDataSet(Globals.PORTS)
				.read()
				.fstream()
				.map(Port::fromRecord)
				.toList();
	}
	
	private Option<Port> findClosestPort(Point pt) {
		return FStream.of(m_ports)
					.map(port -> Tuple.of(port, calcDistance(pt, port.m_loc)))
					.filter(t -> Double.compare(t._2, t._1.m_radius) <= 0)
					.min(t -> t._2)
					.map(t -> t._1);
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
		
		@Override
		public String toString() {
			return String.format("%s:%f:%f:%f)",
								m_name, m_loc.getX(), m_loc.getY(), m_radius);
		}
		
		private static Port fromRecord(Record record) {
			return new Port(record.getString("port_name"),
							(Point)record.getGeometry("the_geom"),
							record.getDouble("radius", -1));
		}
	}
}
