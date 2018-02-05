package debs;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.geotools.referencing.GeodeticCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Point;

import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.mapreduce.support.MarmotMRContexts;
import marmot.optor.Sort;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.type.DataType;
import marmot.type.Trajectory;
import marmot.type.Trajectory.Sample;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShipTrajectoryGenerator extends AbstractRecordSetFunction implements Serializable {
	private static final long serialVersionUID = 3136715462538083686L;
	private static final Logger s_logger = LoggerFactory.getLogger(ShipTrajectoryGenerator.class);
	
	private static final RecordSchema SCHEMA
							= RecordSchema.builder()
											.addColumn("trajectory", DataType.TRAJECTORY)
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

	private static final long MAX_DELAY = TimeUnit.HOURS.toMillis(1);	// 1 hour
	private static final double MAX_VALID_DISTANCE = 3_600;	// 60km/h
	private static final long MILLIS_PER_MIN = TimeUnit.MINUTES.toMillis(1);
	private static final long MIN_NAVI_DURATION = TimeUnit.HOURS.toMillis(3);
	private static class Created extends SingleInputRecordSet<ShipTrajectoryGenerator> {
		private final Record m_inputRecord;
		private Sample m_lastSample;
		
		private final GeodeticCalculator m_gc = new GeodeticCalculator();
		
		Created(ShipTrajectoryGenerator gen, RecordSet input) {
			super(gen, input);
			
			m_inputRecord = newInputRecord();
		}

		@Override
		public boolean next(Record record) throws RecordSetException {
			Trajectory.Builder builder = Trajectory.builder();
			
			if ( m_lastSample == null ) {
				if ( !m_input.next(m_inputRecord) ) {
					return false;
				}
				
				m_lastSample = toSample(m_inputRecord);
				
			}
			builder.add(m_lastSample);
			
			Sample sample = null;
			while ( true ) {
				boolean eor;
				while ( !(eor = !m_input.next(m_inputRecord)) ) {
					sample = toSample(m_inputRecord);
					
					long timeGap = sample.getMillis() - m_lastSample.getMillis();
					if ( timeGap > MAX_DELAY ) {
						break;
					}
					
					double distPerMin = calcDistancePerMinute(m_lastSample, sample, timeGap);
					if ( distPerMin <= MAX_VALID_DISTANCE ) {
						builder.add(sample);
						m_lastSample = sample;
					}
				}
				m_lastSample = sample;
				
				Trajectory trj = builder.build();
				if ( trj.getDuration().getMillis() >= MIN_NAVI_DURATION ) {
					record.set(0, trj);
					return true;
				}
				if ( eor ) {
					return false;
				}
			}
		}
		
		private Sample toSample(Record record) {
			Point pt = (Point)record.getGeometry("the_geom");
			long ts = record.getLong("ts", -1);
			
			return new Sample(pt, ts);
		}
		
		private double calcDistancePerMinute(Sample sample1, Sample sample2, long gap) {
			Point src = sample1.getPoint();
			Point tar = sample2.getPoint();
			m_gc.setStartingGeographicPoint(src.getX(), src.getY());
			m_gc.setDestinationGeographicPoint(tar.getX(), tar.getY());
			return m_gc.getOrthodromicDistance() / gap * MILLIS_PER_MIN;
		}
	}

}
