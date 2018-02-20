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
import marmot.optor.Sort;
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
											.addColumn("dest_port", DataType.STRING)
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
		
		input = Sort.by("ts:A").apply(m_marmot, input);
		
		return new Created(this, input);
	}

	private static final double MAX_VALID_DISTANCE = 100;			// 100 nmile
	private static final double MAX_VALID_SPEED = 50;				// 50 kt
	private static final long MILLIS_PER_HOUR = TimeUnit.HOURS.toMillis(1);
	
	private static class Created extends SingleInputRecordSet<ShipTrajectoryGenerator> {
		private final Record m_inputRecord;
		private Sample m_lastSample;

		private final GeodeticCalculator m_gc = new GeodeticCalculator();
		private Trajectory.Builder m_builder = Trajectory.builder();
		
		Created(ShipTrajectoryGenerator gen, RecordSet input) {
			super(gen, RecordSets.toPeekable(input));
			
			m_inputRecord = newInputRecord();
			if ( m_input.next(m_inputRecord) ) {
				m_lastSample = toSample(m_inputRecord);
				m_builder.add(m_lastSample);
			}
			
		}

		@Override
		public boolean next(Record record) throws RecordSetException {
			Sample sample = null;
			while ( true ) {
				boolean eor;
				while ( !(eor = !m_input.next(m_inputRecord)) ) {
					sample = toSample(m_inputRecord);
					
					Duration interval = Duration.ofMillis(sample.getMillis() - m_lastSample.getMillis());
					if ( interval.isZero() ) {
						s_logger.debug("skip same timestamp: ts={}", sample.getMillis());
						continue;
					}

					double dist = calcDistance(m_lastSample.getPoint(), sample.getPoint()) / 1_852;
					double speed = (dist / interval.toMillis()) * MILLIS_PER_HOUR;

					if ( speed > MAX_VALID_SPEED && dist > MAX_VALID_DISTANCE ) {
						Option<Record> orec = ((PeekableRecordSet)m_input).peek();
						if ( orec.isDefined() ) {
							Sample next = toSample(orec.get());
							double dist2 = calcDistance(m_lastSample.getPoint(), next.getPoint()) / 1_852;
							if ( dist2/2 < dist ) {
								double dist3 = calcDistance(sample.getPoint(), next.getPoint()) / 1_852;
								s_logger.info(String.format("drop spike track: %.1f-%.1f vs. %.1f",
															dist, dist3, dist2));
								
								continue;
							}
						}
						s_logger.info(String.format("break into a trajectory: "
												+ "speed=%.2fkt dist=%.2fnmile interval=%s%n",
												speed, dist, interval));
						break;
					}
					
					m_builder.add(sample);
					m_lastSample = sample;
				}
				m_lastSample = sample;
				
				if ( eor && m_builder.length() == 0 ) {
					return false;
				}
				if ( m_builder.length() > 0 ) {
					Trajectory traj = m_builder.build();
					m_builder.clear();
					
					record.set(0, traj);
					
					return true;
				}
			}
		}
		
		private Sample toSample(Record record) {
			Point pt = (Point)record.getGeometry("the_geom");
			long ts = record.getLong("ts", -1);
			
			return new Sample(pt, ts);
		}
		
		private double calcDistance(Point pt1, Point pt2) {
			m_gc.setStartingGeographicPoint(pt1.getX(), pt1.getY());
			m_gc.setDestinationGeographicPoint(pt2.getX(), pt2.getY());
			return m_gc.getOrthodromicDistance();
		}
	}
}
