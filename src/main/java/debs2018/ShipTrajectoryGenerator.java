package debs2018;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Point;

import io.vavr.control.Option;
import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.rset.AbstractRecordSet;
import marmot.type.DataType;
import marmot.type.Trajectory;
import marmot.type.Trajectory.Sample;
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
											.addColumn("trajectory", DataType.TRAJECTORY)
											.addColumn("depart_port", DataType.STRING)
											.build();
	
	@Override
	public void initialize(MarmotRuntime marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, SCHEMA);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		if ( m_marmot == null ) {
			throw new IllegalArgumentException("not initialized: optor=" + this);
		}
		
		return new Created(this, input);
	}

	private static final Duration MAX_VALID_DURATION = Duration.ofHours(2);	// 10 hours
	private static final double MAX_VALID_DISTANCE = 100;			// 100 nmile
	private static final double MAX_VALID_SPEED = 50;				// 50 kt
	private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
	
	private static class Created extends AbstractRecordSet {
		private final FStream<List<ShipTrack>> m_buffereds;
		private final Ports m_ports;
		private final GeodeticCalculator m_gc = new GeodeticCalculator();
		
		private Trajectory.Builder m_builder = Trajectory.builder();
		
		Created(ShipTrajectoryGenerator gen, RecordSet input) {
			m_ports = Ports.load(gen.m_marmot);
			m_buffereds = input.fstream()
								.map(ShipTrack::new)
								.buffer(3, 1);
			
			Option<List<ShipTrack>> obuffer = m_buffereds.next();
			obuffer.forEach(b -> m_builder.add(b.get(0).toSample()));
		}

		@Override
		public RecordSchema getRecordSchema() {
			return SCHEMA;
		}

		@Override
		public boolean next(Record record) throws RecordSetException {
			Option<List<ShipTrack>> obuffer;
			String departPort = null;
			
			while ( true ) {
				while ( (obuffer = m_buffereds.next()).isDefined() ) {
					List<ShipTrack> buffer = obuffer.get();
					if ( buffer.size() <= 1 ) {
						break;
					}
					
					ShipTrack t0 = buffer.get(0);
					ShipTrack t1 = buffer.get(1);

					departPort = t0.m_departPort;
					if ( !t0.m_departPort.equals(t1.m_departPort) ) {
						break;
					}
					
					double dist01 = t1.distanceTo(t0) / 1_852;
					Duration interval01 = interval(t0, t1);
					double speed01 = (dist01 / interval01.toMillis()) * MILLIS_PER_HOUR;
					
					if ( speed01 > MAX_VALID_SPEED && dist01 > MAX_VALID_DISTANCE ) {
						if ( buffer.size() > 2 ) {
							ShipTrack t2 = buffer.get(2);
							double dist12 = t1.distanceTo(t2) / 1_852;
							double dist02 = t0.distanceTo(t2) /  1_852;
							
							if ( dist02 < dist12/2 ) {
								s_logger.info(String.format("drop spike track: %.1f-%.1f vs. %.1f",
															dist01, dist12, dist02));
								
								continue;
							}
						}
					}
					
					if ( interval01.compareTo(MAX_VALID_DURATION) > 0 ) {
						Option<Port> nearest0 = m_ports.findNearestValidPort(t0.m_loc);
						Option<Port> nearest1 = m_ports.findNearestValidPort(t1.m_loc);
						if ( nearest0.isDefined() && !nearest0.equals(nearest1)
							&& !nearest0.get().m_name.equals(t0.m_departPort)) {
							departPort = t0.m_departPort;
							break;
						}
					}
	
					m_builder.add(t1.toSample());
				}
				
				Trajectory traj =  m_builder.length() > 0 ? m_builder.build() : null;
				m_builder.clear();
				
				if ( traj != null ) {
					record.set(0, traj);
					record.set(1, departPort);
					
					return true;
				}
				else {
					return false;
				}
			}
		}
		
		private class ShipTrack {
			private final String m_departPort;
			private final Point m_loc;
			private final long m_ts;
			
			ShipTrack(Record record) {
				m_departPort = record.getString("depart_port");
				m_loc = (Point)record.getGeometry("the_geom");
				m_ts = record.getLong("ts", -1);
			}
			
			Sample toSample() {
				return new Sample(m_loc, m_ts);
			}
			
			double distanceTo(ShipTrack track) {
				m_gc.setStartingGeographicPoint(m_loc.getX(), m_loc.getY());
				m_gc.setDestinationGeographicPoint(track.m_loc.getX(), track.m_loc.getY());
				return m_gc.getOrthodromicDistance();
			}
			
			@Override
			public boolean equals(Object obj) {
				if ( this == obj ) {
					return true;
				}
				else if ( obj == null || getClass() != obj.getClass() ) {
					return false;
				}
				
				ShipTrack other = (ShipTrack)obj;
				return Objects.equals(m_ts, other.m_ts)
						&& Objects.equals(m_loc, other.m_loc)
						&& Objects.equals(m_departPort, other.m_departPort);
			}
			
			@Override
			public int hashCode() {
				return Objects.hash(m_departPort, m_loc, m_ts);
			}
			
			@Override
			public String toString() {
				return String.format("%s->:%s, (%s)",
									m_departPort, m_loc, m_ts);
			}
		}
		
		Duration interval(ShipTrack begin, ShipTrack end) {
			return Duration.ofMillis(end.m_ts - begin.m_ts);
		}
	}
}
