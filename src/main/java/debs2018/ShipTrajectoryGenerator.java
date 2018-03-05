package debs2018;

import java.io.Serializable;
import java.time.Duration;
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
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.rset.PeekableRecordSet;
import marmot.rset.RecordSets;
import marmot.type.DataType;
import marmot.type.Trajectory;
import marmot.type.Trajectory.Sample;

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

	private static final Duration MAX_VALID_DURATION = Duration.ofHours(10);	// 10 hours
	private static final double MAX_VALID_DISTANCE = 100;			// 100 nmile
	private static final double MAX_VALID_SPEED = 50;				// 50 kt
	private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
	
	private static class Created extends SingleInputRecordSet<ShipTrajectoryGenerator> {
		private final Record m_inputRecord;
		private ShipTrack m_lastTrack;

		private Ports m_ports;
		private final GeodeticCalculator m_gc = new GeodeticCalculator();
		private Trajectory.Builder m_builder = Trajectory.builder();
		
		Created(ShipTrajectoryGenerator gen, RecordSet input) {
			super(gen, RecordSets.toPeekable(input));
			
			m_ports = Ports.load(gen.m_marmot);
			
			m_inputRecord = newInputRecord();
			if ( m_input.next(m_inputRecord) ) {
				m_lastTrack = new ShipTrack(m_inputRecord);
				m_builder.add(m_lastTrack.toSample());
			}
		}

		@Override
		public boolean next(Record record) throws RecordSetException {
			ShipTrack track = null;
			
			while ( true ) {
				boolean eor;
				while ( !(eor = !m_input.next(m_inputRecord)) ) {
					track = new ShipTrack(m_inputRecord);
					
					if ( !track.m_departPort.equals(m_lastTrack.m_departPort) ) {
System.out.printf("%s -> %s%n", m_lastTrack.m_departPort, track.m_departPort);
						record.set(1, m_lastTrack.m_departPort);
						break;
					}
					
					Duration interval = interval(m_lastTrack, track);
					if ( interval.compareTo(MAX_VALID_DURATION) > 0 ) {
						Option<Port> nearest = m_ports.findNearestValidPort(m_lastTrack.m_loc);
						if ( nearest.isDefined() ) {
							record.set(1, m_lastTrack.m_departPort);
							break;
						}
					}

					double dist = m_lastTrack.distanceTo(track) / 1_852;
					double speed = (dist / interval.toMillis()) * MILLIS_PER_HOUR;
					
					if ( speed > MAX_VALID_SPEED && dist > MAX_VALID_DISTANCE ) {
						Option<Record> orec = ((PeekableRecordSet)m_input).peek();
						if ( orec.isDefined() ) {
							ShipTrack next = new ShipTrack(orec.get());
							
							double dist2 = m_lastTrack.distanceTo(next) /  1_852;
							if ( dist2/2 < dist ) {
								double dist3 = track.distanceTo(next) /  1_852;
								s_logger.info(String.format("drop spike track: %.1f-%.1f vs. %.1f",
															dist, dist3, dist2));
								
								continue;
							}
						}
						s_logger.info(String.format("break into a trajectory: "
												+ "speed=%.2fkt dist=%.2fnmile interval=%s%n",
												speed, dist, interval));
						record.set(1, m_lastTrack.m_departPort);
						break;
					}
					
					m_builder.add(track.toSample());
					m_lastTrack = track;
				}
				
				Trajectory traj =  m_builder.length() > 0 ? m_builder.build() : null;
				m_builder.clear();
				
				if ( !eor ) {
					m_lastTrack = track;
					m_builder.add(m_lastTrack.toSample());
				}
//				else {
//					System.out.println("EOF");
//				}
				
				if ( traj != null ) {
					record.set(0, traj);
					
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
		}
		
		Duration interval(ShipTrack begin, ShipTrack end) {
			return Duration.ofMillis(end.m_ts - begin.m_ts);
		}
	}
}
