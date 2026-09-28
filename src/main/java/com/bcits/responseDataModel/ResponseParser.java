package com.bcits.responseDataModel;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.bcits.dao.NamePlateDao;
import com.bcits.dao.NamePlateHstDao;
import com.bcits.dao.SchduledMeterTrackDao;
import com.bcits.dataCollection.BillProcessor;
import com.bcits.dataCollection.BlockLoadProcessor;
import com.bcits.dataCollection.CommunicationTracker;
import com.bcits.dataCollection.DailyLoadProcessor;
import com.bcits.dataCollection.EventProcessor;
import com.bcits.dataCollection.InstantaneousProcessor;
import com.bcits.dataCollection.MeterAlarmProcessor;
import com.bcits.dataCollection.NamePlateProcessor;
import com.bcits.dataCollection.SequenceTracker;
import com.bcits.dataModel.DeviceData;
import com.bcits.dataModel.PreVEE;
import com.bcits.dataModel.ProcessingStats;
import com.bcits.dataModel.RegisterValue;
import com.bcits.dataModel.RegisterValues;
import com.bcits.utils.GenericExceptionTracker;
import com.bcits.utils.Subscriber;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class ResponseParser {

	private static final Logger log = LoggerFactory.getLogger(ResponseParser.class);

	@PersistenceContext
	protected EntityManager entityManager;
	@Autowired
	SchduledMeterTrackDao schMtrTrckDao;
	@Autowired
	NamePlateDao namePlateDao;
	@Autowired
	NamePlateHstDao namePlateHstDao;
	@Autowired
	private KafkaTrackerService kafkaTrackerService;
	private final ThreadLocal<Integer> count_tl = ThreadLocal.withInitial(() -> 0);
	private final ThreadLocal<Integer> totalCount_tl = ThreadLocal.withInitial(() -> 0);
	private Timestamp event_time = null;
	private String meter_serial_number = null, device_id = null, manufacturer_name = null, firmware_version = null,
			meter_type = null;
	private String meter_catagory = null, current_rating = null, year_of_manufacture = null, ct_ratio = null,
			pt_ratio = null;
	Double ctratio = null;
	Double ptratio = null;
	private RegisterValues[] rv;
	private String obc;
	private String val;
	private List<RegisterValue> dt;
	private String dType;
	private PreVEE[] preVdt;
	private Gson gson;

	@Autowired
	private CommunicationTracker commTracker;
	private final ThreadLocal<BlockLoadProcessor> blockLoadProcessor = ThreadLocal.withInitial(BlockLoadProcessor::new);
	private final ThreadLocal<InstantaneousProcessor> instantaneousProcessor = ThreadLocal.withInitial(InstantaneousProcessor::new);
	private final ThreadLocal<BillProcessor> billProcessor = ThreadLocal.withInitial(BillProcessor::new);
	private final ThreadLocal<EventProcessor> eventProcessor = ThreadLocal.withInitial(EventProcessor::new);
	private final ThreadLocal<DailyLoadProcessor> dailyLoadProcessor = ThreadLocal.withInitial(DailyLoadProcessor::new);
	private final ThreadLocal<NamePlateProcessor> namePlateProcessor = ThreadLocal.withInitial(NamePlateProcessor::new);
	private final ThreadLocal<MeterAlarmProcessor> meterAlarmProcessor = ThreadLocal.withInitial(MeterAlarmProcessor::new);
	// public static BHESMeterAlarmProcessor bhesMeterAlarmProcessor =
	// BHESMeterAlarmProcessor.getInstance();
	public static SequenceTracker seqTracker = SequenceTracker.getInstance();

	public static StringBuilder query = new StringBuilder();
	public static StringBuilder missing_query = new StringBuilder();
	public static String type = null;
	private String meter_number = null, missing_type = null, missing_meter_number = null, sql = null;
	private long seq_id = 0, aseq_id = 0, missingseq_id = 0, gwseq_id;
	private int count, alarmCount, missingSeqCount, gCount;
	private long maxSeq, maxAlarmSeq, missingmaxSeq, maxGatewaySeq;
	private int totalCount, totalAlarmCount, totalMissingSeqCount, totalGatewayCount;
	public static StringBuilder exceptionQuery = new StringBuilder();

	private final ExecutorService schPool = Executors.newFixedThreadPool(10);
	private final ExecutorService trackPool = Executors.newFixedThreadPool(5);
	private final ExecutorService expPool = Executors.newFixedThreadPool(5);
	private final ExecutorService bLoadPool = Executors.newFixedThreadPool(10);
	private final ExecutorService instPool = Executors.newFixedThreadPool(10);
	private final ExecutorService billPool = Executors.newFixedThreadPool(10);
	private final ExecutorService dLoadPool = Executors.newFixedThreadPool(10);
	private final ExecutorService eventPool = Executors.newFixedThreadPool(10);
	private final ExecutorService alarmPool = Executors.newFixedThreadPool(10);
	private final ExecutorService nmPool = Executors.newFixedThreadPool(10);

	// @Transactional
	// @Async
	// public int parseData(List<DeviceData> smm) throws ParseException,
	// InterruptedException {
	// totalCount=0;
	// blockLoadProcessor.init();
	// instantaneousProcessor.init();
	// billProcessor.init();
	// eventProcessor.init();
	// dailyLoadProcessor.init();
	// namePlateProcessor.init();
	//
	// query.setLength(0);//To Make String Builder as Null
	// query.append("insert into meter_data.sch_meter_trackload (time_stamp ,seq_id,
	// type, meter_number) values ");
	//
	// exceptionQuery.setLength(0);
	// exceptionQuery.append("insert into meter_data.exception_seq_tracker
	// (time_stamp, seq_id, type, data) values ");
	// Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
	// boolean isStringContainsSpecialCharacter=true;
	//
	// for (DeviceData scMM : smm) {
	//// if(scMM.getSequenceNumber() !=null) {
	//// seq_id=Long.parseLong(scMM.getSequenceNumber());
	//// }else {
	//// seq_id=Long.parseLong(scMM.getMeterSampleId());
	//// }
	// gson = new Gson();
	// String smm1 = gson.toJson(scMM, new TypeToken<DeviceData>() {}.getType());
	//
	// seq_id=scMM.getSeqId();
	// meter_number=scMM.getDeviceId();
	//
	// Matcher matcher = pattern.matcher(meter_number);
	// isStringContainsSpecialCharacter = matcher.find();
	//
	// if (!isStringContainsSpecialCharacter) {
	//
	// if (scMM.getProfileCode().equalsIgnoreCase("1.0.99.1.0.255")) { // bulk load
	// type = "Bulk Load";
	// blockLoadProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("1.0.94.91.0.255")) {//
	// instantaneous
	// type = "Instantaneous";
	// instantaneousProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("1.0.98.1.0.255")) {//
	// billing
	// type = "Billing";
	// billProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.0.255")) {//
	// voltage event log
	// type = "Voltage Event Log";
	// eventProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.1.255")) {//
	// current event log
	// type = "Current Event Log";
	// eventProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.2.255")) {//
	// power event log
	// type = "Power Event Log";
	// eventProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.3.255")) {//
	// Transaction Event
	// // Log
	// type = "Transaction Event Log";
	// eventProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.4.255")) {//
	// other event log
	// type = "Other Event Log";
	// eventProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.5.255")) { //
	// Non Roll Over
	// // Event Log
	// type = "Non Roll Over Event Log";
	// eventProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.6.255")) {//
	// Controlled event
	// // log
	// type = "Controled Event Log";
	// eventProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("1.0.99.2.0.255")) { //
	// Daily load
	// type = "Daily Load";
	// dailyLoadProcessor.addData(scMM);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.94.91.10.255")) {//
	// Name plate
	// type = "Name Plate";
	// namePlateProcessor.addData(scMM);
	// }
	//
	// query.append("(now()," + seq_id + ", '" + type + "', '" + meter_number + "'
	// ),");
	// commTracker.add(meter_number);
	//
	// }else {
	//// System.out.println("Exception Query Block");
	// exceptionQuery.append("(now(),'" + scMM.getDeviceId() + "','" +
	// scMM.getProfileCode() + "','" + smm1.toString() + "'),");
	// }
	//
	// if(maxSeq<seq_id) {
	// maxSeq=seq_id;
	// }
	// }
	// maxSeq++;
	// Thread.sleep(1000);
	//// HESintegration.dataCollector= new DataCollector(maxSeq);
	//// HESintegration.dataCollector.start();
	// smm=null;
	//
	//
	// if (blockLoadProcessor.hasData()) {
	// try {
	// if(!blockLoadProcessor.getQuery().contains("values ON")) {
	//// System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	// synchronized (this) {
	// String s=blockLoadProcessor.getQuery();
	// Runnable data=()->{count = schMtrTrckDao.bulkInsert(s);};
	// bLoadPool.execute(data);
	// totalCount += count;
	// }
	//
	// //totalCount += count =
	// schMtrTrckDao.bulkInsert(blockLoadProcessor.getQuery());
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// System.out.println("BCITS HES Load Count :: " + count + " " + new
	// Timestamp(new Date().getTime()));
	// count=0;
	// }
	// if(blockLoadProcessor.hasExcpData()) {
	// if(!blockLoadProcessor.getExcepQuery().contains("values ON")) {
	// synchronized (this) {
	// String s=blockLoadProcessor.getExcepQuery();
	// Runnable data=()->{count=schMtrTrckDao.bulkInsert(s);};
	// expPool.execute(data);
	// }
	// }
	// System.out.println("BCITS HES Exception Load Count :: " + count + " " + new
	// Timestamp(new Date().getTime()));
	// count=0;
	// }
	// blockLoadProcessor.reset();
	//
	// if (instantaneousProcessor.hasData()) {
	// try {
	// if(!instantaneousProcessor.getQuery().contains("values ON")) {
	//// System.out.println("Main Query:- "+instantaneousProcessor.getQuery());
	//// totalCount += count =
	// schMtrTrckDao.bulkInsert(instantaneousProcessor.getQuery());
	// synchronized (this) {
	// String insQ=instantaneousProcessor.getQuery();
	// Runnable instdata=()->{count = schMtrTrckDao.bulkInsert(insQ);};
	// instPool.execute(instdata);
	// totalCount += count;
	// }
	//
	// }
	// } catch (Exception e) {
	//
	// }
	// System.out.println("BCITS HES Insta Count :: " + count + " " + new
	// Timestamp(new Date().getTime()));
	// count=0;
	// }
	// if(instantaneousProcessor.hasExcpData()) {
	//// System.out.println("Exception Query:-
	// "+instantaneousProcessor.getExcepQuery());
	// if(!instantaneousProcessor.getExcepQuery().contains("values ON")) {
	//// count =schMtrTrckDao.bulkInsert(instantaneousProcessor.getExcepQuery());
	// synchronized (this) {
	// String s=instantaneousProcessor.getExcepQuery();
	// Runnable data=()->{count =schMtrTrckDao.bulkInsert(s);};
	// expPool.execute(data);
	// }
	// }
	// System.out.println("BCITS HES Insta Exception Count :: " + count + " " + new
	// Timestamp(new Date().getTime()));
	// count=0;
	// }
	// instantaneousProcessor.reset();
	//
	// if (billProcessor.hasData()) {
	// try {
	// if(!billProcessor.getQuery().contains("values ON")) {
	//// totalCount += count = schMtrTrckDao.bulkInsert(billProcessor.getQuery());
	// synchronized (this) {
	// String billQ=billProcessor.getQuery();
	// Runnable bdata=()->{count = schMtrTrckDao.bulkInsert(billQ);};
	// billPool.execute(bdata);
	// totalCount += count;
	// }
	//
	// }
	// } catch (Exception e) {
	//
	// }
	// System.out.println("BCITS HES Bill Count :: " + count + " " + new
	// Timestamp(new Date().getTime()));
	// count=0;
	// }
	// if(billProcessor.hasExcpData()) {
	// if(!billProcessor.getExcepQuery().contains("values ON")) {
	//// schMtrTrckDao.bulkInsert(billProcessor.getExcepQuery());
	// synchronized (this) {
	// String s=billProcessor.getExcepQuery();
	// Runnable data=()->{count =schMtrTrckDao.bulkInsert(s);};
	// expPool.execute(data);
	// }
	// }
	// System.out.println("BCITS HES Exception Bill Count :: " + count + " " + new
	// Timestamp(new Date().getTime()));
	// count=0;
	// }
	// billProcessor.reset();
	//
	// if (eventProcessor.hasData()) {
	// try {
	// if(!eventProcessor.getQuery().contains("values ON")) {
	// totalCount += count = schMtrTrckDao.bulkInsert(eventProcessor.getQuery());
	// }
	// } catch (Exception e) {
	//
	// }
	// System.out.println("BCITS HES Event Count :: " + count + " " + new
	// Timestamp(new Date().getTime()));
	// }
	// if(eventProcessor.hasExcpData()) {
	// if(!eventProcessor.getExcepQuery().contains("values ON")) {
	// schMtrTrckDao.bulkInsert(eventProcessor.getExcepQuery());
	// }
	//
	// }
	// eventProcessor.reset();
	//
	// if(dailyLoadProcessor.hasData()) {
	// try {
	// if(!dailyLoadProcessor.getQuery().contains("values ON")) {
	//// totalCount+=count =
	// schMtrTrckDao.bulkInsert(dailyLoadProcessor.getQuery());
	// synchronized (this) {
	// String dLoadQ=dailyLoadProcessor.getQuery();
	// Runnable dailydata=()->{count = schMtrTrckDao.bulkInsert(dLoadQ);};
	// dLoadPool.execute(dailydata);
	// totalCount += count;
	// }
	//
	// }
	// System.out.println("BCITS HES DailyLoad Count :: "+count+ " "+ new
	// Timestamp(new Date().getTime()));
	// count=0;
	// } catch (Exception e) {
	//
	// }
	// }
	// if(dailyLoadProcessor.hasExcpData()) {
	// if(!dailyLoadProcessor.getExcepQuery().contains("values ON")) {
	//// schMtrTrckDao.bulkInsert(dailyLoadProcessor.getExcepQuery());
	// synchronized (this) {
	// String s=dailyLoadProcessor.getExcepQuery();
	// Runnable data=()->{count =schMtrTrckDao.bulkInsert(s);};
	// expPool.execute(data);
	// }
	// }
	// System.out.println("BCITS HES DailyLoad Exception Count :: " + count + " " +
	// new Timestamp(new Date().getTime()));
	// count=0;
	// }
	// dailyLoadProcessor.reset();
	//
	// if(namePlateProcessor.hasData()) {
	// if(!namePlateProcessor.getQuery().contains("values ON")) {
	// totalCount+=count = schMtrTrckDao.bulkInsert(namePlateProcessor.getQuery());
	// }
	//
	// System.out.println("NamePlate Count :: "+count+ " "+ new Timestamp(new
	// Date().getTime()));
	// }
	// if(namePlateProcessor.hasExcpData()) {
	// if(!namePlateProcessor.getExcepQuery().contains("values ON")) {
	// schMtrTrckDao.bulkInsert(namePlateProcessor.getExcepQuery());
	// }
	//
	// }
	// namePlateProcessor.reset();
	//
	// if(isStringContainsSpecialCharacter) {
	//// System.out.println(exceptionQuery.toString().substring(0,
	// exceptionQuery.length() - 1));
	// schMtrTrckDao.bulkInsert(exceptionQuery.toString().substring(0,
	// exceptionQuery.length() - 1)+" ON CONFLICT DO NOTHING");
	// exceptionQuery.setLength(0);
	// }else {
	//// System.out.println(query.toString().substring(0, query.length() - 1));
	// count=schMtrTrckDao.bulkInsert(query.toString().substring(0, query.length() -
	// 1));
	// query.setLength(0);//Reset
	// System.out.println("----------SchTracker Count "+count+ " "+ new
	// Timestamp(new Date().getTime()));
	//
	// }
	//
	//
	// String sch=query.toString().substring(0, query.length() - 1);
	// Runnable schdata=()->{count = schMtrTrckDao.bulkInsert(sch);};
	// query.setLength(0);
	// schPool.execute(schdata);
	// count=0;
	//
	//
	//
	//
	// if (!commTracker.isEmpty()) {
	// String comQ=commTracker.getQuery();
	// Runnable comdata=()->{count = schMtrTrckDao.bulkInsert(comQ);};
	// trackPool.execute(comdata);
	// System.out.println("----------Total Meters Updated to Tracker :: " + count +
	// " "+ new Timestamp(new Date().getTime()));
	// commTracker.reset();
	// count=0;
	// }
	//
	// if (!seqTracker.isEmpty()) {
	// schMtrTrckDao.bulkInsert(seqTracker.getQuery());
	// seqTracker.reset();// reseting the tracker after update
	// }
	//
	// return totalCount;
	// }

	public long getSavedOffset(String topic, int partition) {
		return schMtrTrckDao.getSavedOffset(topic, partition);
	}

	public void saveOffsets(Map<TopicPartition, Long> partitionOffsetMap, String hes) {
		// TODO Auto-generated method stub
		schMtrTrckDao.saveOffsets(partitionOffsetMap, hes);

	}

	@Transactional
	@Async
	// public void parseData(List<DeviceData> smm, String topic, String hes, int
	// partition, long offset) throws ParseException, InterruptedException {
	// ProcessingStats stats = new ProcessingStats();
	// stats.setTotalRecords(smm.size());
	// ArrayList<String> alrmDesc = null;
	// totalCount = 0;
	// blockLoadProcessor.init();
	// instantaneousProcessor.init();
	// billProcessor.init();
	// eventProcessor.init();
	// dailyLoadProcessor.init();
	// namePlateProcessor.init();
	// meterAlarmProcessor.init();
	//
	// query.setLength(0);// To Make String Builder as Null
	// query.append("insert into meter_data.sch_meter_trackload (time_stamp ,seq_id,
	// type, meter_number,apiname,hes_type) values ");
	// exceptionQuery.setLength(0);
	// exceptionQuery.append("insert into meter_data.exception_seq_tracker
	// (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");
	// Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
	// boolean isStringContainsSpecialCharacter = true;
	// boolean ispreVeeCharacter = false;
	// List<String> meterNosForNamePlate = new ArrayList<>();
	// Gson gson = new Gson();
	//
	// try {
	// for (DeviceData scMM : smm) {
	// gson = new Gson();
	// String smm1 = gson.toJson(scMM, new TypeToken<DeviceData>() {
	// }.getType());
	//
	// seq_id = scMM.getSeqId();
	// meter_number = scMM.getDeviceId();
	//
	// Matcher matcher = pattern.matcher(meter_number);
	// isStringContainsSpecialCharacter = matcher.find();
	// if (!isStringContainsSpecialCharacter) {
	//
	// if (scMM.getProfileCode().equalsIgnoreCase("1.0.99.1.0.255")) { // bulk load
	// type = "Bulk Load";
	// if (scMM.getPreVEE() != null) {
	// blockLoadProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	//
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.25.9.0.255")) {//
	// instantaneous push
	// type = "Instantaneous Push";
	// if (scMM.getPreVEE() != null) {
	// instantaneousProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// instantaneousProcessor.addData(scMM, hes);
	// } else if (scMM.getProfileCode().equalsIgnoreCase("1.0.94.91.0.255")) {//
	// instantaneous Pull
	// type = "Instantaneous Pull";
	// if (scMM.getPreVEE() != null) {
	// instantaneousProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// instantaneousProcessor.addData(scMM, hes);
	// }
	//
	// else if (scMM.getProfileCode().equalsIgnoreCase("1.0.94.91.0.255")) {//
	// instantaneous
	// type = "Instantaneous";
	//
	// if (scMM.getPreVEE() != null) {
	// instantaneousProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("1.0.98.1.0.255")) {//
	// billing
	// type = "Billing";
	//
	// if (scMM.getPreVEE() != null) {
	// billProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.0.255")) {//
	// voltage event log
	// type = "Voltage Event Log";
	//
	// if (scMM.getPreVEE() != null) {
	// eventProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.1.255")) {//
	// current event log
	// type = "Current Event Log";
	// if (scMM.getPreVEE() != null) {
	// eventProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.2.255")) {//
	// power event log
	// type = "Power Event Log";
	// if (scMM.getPreVEE() != null) {
	// eventProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.3.255")) {//
	// Transaction Event
	// // Log
	// type = "Transaction Event Log";
	// if (scMM.getPreVEE() != null) {
	// eventProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.4.255")) {//
	// other event log
	// type = "Other Event Log";
	// if (scMM.getPreVEE() != null) {
	// eventProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.5.255")) { //
	// Non Roll Over
	// // Event Log
	// type = "Non Roll Over Event Log";
	// if (scMM.getPreVEE() != null) {
	// eventProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.99.98.6.255")) {//
	// Controlled event
	// // log
	// type = "Controled Event Log";
	// if (scMM.getPreVEE() != null) {
	// eventProcessor.addData(scMM, hes);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("1.0.99.2.0.255")) { //
	// Daily load
	// type = "Daily Load";
	// if (scMM.getPreVEE() != null) {
	// dailyLoadProcessor.addData(scMM, hes);
	// } else {
	// ispreVeeCharacter = true;
	// exceptionQuery.append(
	// "(now(),'" + scMM.getSeqId() + "','" + type + "','" + smm1.toString() +
	// "','"+hes+"'),");
	//// System.out.println("preVEE list is null for device: " +
	// scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.0.94.91.10.255")) {//
	// Name plate
	// type = "Name Plate";
	//
	// if (scMM.getPreVEE() != null) {
	// namePlateProcessor.addData(scMM, hes);
	// meterNosForNamePlate.add(meter_number);
	// } else {
	// System.out.println("preVEE list is null for device: " + scMM.getDeviceId());
	// }
	// } else if (scMM.getProfileCode().equalsIgnoreCase("0.4.25.9.0.255")) {//
	// Meter Alarm
	// type = "Meter Alarm";
	// meterAlarmProcessor.addData(scMM, hes);
	//
	// }
	// query.append("(now()," + seq_id + ", '" + type + "', '" + meter_number +
	// "','"+topic+"','"+hes+"' ),");
	// commTracker.add(meter_number, type);
	// stats.incrementSuccess();
	// stats.addType(type);
	// } else {
	// List<String> exceptions =
	// GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
	// String combinedExceptions = String.join(" | ", exceptions);
	// exceptionQuery.append("(now(),'" + scMM.getSeqId() + "','" + topic + "','" +
	// (scMM.getDeviceId() != null ? scMM.getDeviceId() : "") + "','" +
	// gson.toJson(scMM, new TypeToken<DeviceData>() {
	// }.getType()) + "','"+ combinedExceptions + "','"+hes+"'),");
	// stats.incrementFailure();
	// stats.addType("Invalid Format");
	// }
	//
	//// else {
	//// exceptionQuery.append("(now(),'" + scMM.getDeviceId() + "','" +
	// scMM.getProfileCode() + "','"
	//// + smm1.toString() + "','"+hes+"'),");
	//// }
	//
	// if (maxSeq < seq_id) {
	// maxSeq = seq_id;
	// }
	// }
	// maxSeq++;
	// Thread.sleep(100);
	// smm = null;
	//
	// if (blockLoadProcessor.hasData()) {
	// try {
	// if (!blockLoadProcessor.getQuery().contains("values ON")) {
	//
	// final String mainQuery = blockLoadProcessor.getQuery();
	// // System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	// Future<Integer> future = bLoadPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(mainQuery);
	// });
	//
	// int insertedCount = future.get(); // WAIT FOR EXECUTION
	// totalCount += insertedCount;
	//
	// System.out.println("" + hes + " HES Load Count :: " + insertedCount + " "
	// + new Timestamp(new Date().getTime()));
	//
	// blockLoadProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (blockLoadProcessor.hasExcpData()) {
	// try {
	// if (!blockLoadProcessor.getExcepQuery().contains("values ON")) {
	//
	// final String excpQuery = blockLoadProcessor.getExcepQuery();
	//
	// Future<Integer> future = expPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(excpQuery);
	// });
	//
	// int excpCount = future.get(); // WAIT FOR EXECUTION
	// stats.setExceptionCount(stats.getExceptionCount() + excpCount);
	// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount + "
	// "
	// + new Timestamp(new Date().getTime()));
	//
	// blockLoadProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (instantaneousProcessor.hasData()) {
	// try {
	// if (!instantaneousProcessor.getQuery().contains("values ON")) {
	//
	// final String mainQuery = instantaneousProcessor.getQuery();
	// // System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	// Future<Integer> future = instPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(mainQuery);
	// });
	//
	// int insertedCount = future.get(); // WAIT FOR EXECUTION
	// totalCount += insertedCount;
	//
	// System.out.println("" + hes + " HES Load Count :: " + insertedCount + " "
	// + new Timestamp(new Date().getTime()));
	//
	// instantaneousProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (instantaneousProcessor.hasExcpData()) {
	// try {
	// if (!instantaneousProcessor.getExcepQuery().contains("values ON")) {
	//
	// final String excpQuery = instantaneousProcessor.getExcepQuery();
	//
	// Future<Integer> future = expPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(excpQuery);
	// });
	//
	// int excpCount = future.get(); // WAIT FOR EXECUTION
	// stats.setExceptionCount(stats.getExceptionCount() + excpCount);
	// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount + "
	// "
	// + new Timestamp(new Date().getTime()));
	//
	// instantaneousProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (billProcessor.hasData()) {
	// try {
	// if (!billProcessor.getQuery().contains("values ON")) {
	//
	// final String mainQuery = billProcessor.getQuery();
	// // System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	// Future<Integer> future = billPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(mainQuery);
	// });
	//
	// int insertedCount = future.get(); // WAIT FOR EXECUTION
	// totalCount += insertedCount;
	//
	// System.out.println("" + hes + " HES Load Count :: " + insertedCount + " "
	// + new Timestamp(new Date().getTime()));
	//
	// billProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (billProcessor.hasExcpData()) {
	// try {
	// if (!billProcessor.getExcepQuery().contains("values ON")) {
	//
	// final String excpQuery = billProcessor.getExcepQuery();
	//
	// Future<Integer> future = expPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(excpQuery);
	// });
	//
	// int excpCount = future.get(); // WAIT FOR EXECUTION
	// stats.setExceptionCount(stats.getExceptionCount() + excpCount);
	// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount + "
	// "
	// + new Timestamp(new Date().getTime()));
	//
	// billProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	// if (eventProcessor.hasData()) {
	// try {
	// if (!eventProcessor.getQuery().contains("values ON")) {
	//
	// final String mainQuery = eventProcessor.getQuery();
	// // System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	// Future<Integer> future = eventPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(mainQuery);
	// });
	//
	// int insertedCount = future.get(); // WAIT FOR EXECUTION
	// totalCount += insertedCount;
	//
	// System.out.println("" + hes + " HES Load Count :: " + insertedCount + " "
	// + new Timestamp(new Date().getTime()));
	//
	// eventProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (eventProcessor.hasExcpData()) {
	// try {
	// if (!eventProcessor.getExcepQuery().contains("values ON")) {
	//
	// final String excpQuery = eventProcessor.getExcepQuery();
	//
	// Future<Integer> future = expPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(excpQuery);
	// });
	//
	// int excpCount = future.get(); // WAIT FOR EXECUTION
	// stats.setExceptionCount(stats.getExceptionCount() + excpCount);
	// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount + "
	// "
	// + new Timestamp(new Date().getTime()));
	//
	// eventProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (dailyLoadProcessor.hasData()) {
	// try {
	// if (!dailyLoadProcessor.getQuery().contains("values ON")) {
	//
	// final String mainQuery = dailyLoadProcessor.getQuery();
	// // System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	// Future<Integer> future = dLoadPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(mainQuery);
	// });
	//
	// int insertedCount = future.get(); // WAIT FOR EXECUTION
	// totalCount += insertedCount;
	//
	// System.out.println("" + hes + " HES Load Count :: " + insertedCount + " "
	// + new Timestamp(new Date().getTime()));
	//
	// dailyLoadProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (dailyLoadProcessor.hasExcpData()) {
	// try {
	// if (!dailyLoadProcessor.getExcepQuery().contains("values ON")) {
	//
	// final String excpQuery = dailyLoadProcessor.getExcepQuery();
	//
	// Future<Integer> future = expPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(excpQuery);
	// });
	//
	// int excpCount = future.get(); // WAIT FOR EXECUTION
	// stats.setExceptionCount(stats.getExceptionCount() + excpCount);
	// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount + "
	// "
	// + new Timestamp(new Date().getTime()));
	//
	// dailyLoadProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	//// if (namePlateProcessor.hasData()) {
	//// try {
	//// if (!namePlateProcessor.getQuery().contains("values ON")) {
	////
	//// List<?> namePlateDetails =
	// GenericExceptionTracker.checknamePlatedetails(meter_number);
	//// if (namePlateDetails != null && !namePlateDetails.isEmpty()) {
	//// GenericExceptionTracker.insertAndDeleteNamePlateData(meter_number);
	//// }
	//// final String mainQuery = namePlateProcessor.getQuery();
	//// // System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	//// Future<Integer> future = nmPool.submit(() -> {
	//// return schMtrTrckDao.bulkInsert(mainQuery);
	//// });
	////
	//// int insertedCount = future.get(); // WAIT FOR EXECUTION
	//// totalCount += insertedCount;
	////
	//// System.out.println("" + hes + " HES Load Count :: " + insertedCount + " "
	//// + new Timestamp(new Date().getTime()));
	////
	//// namePlateProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	//// }
	//// } catch (Exception e) {
	//// e.printStackTrace();
	//// }
	//// }
	////
	//// if (namePlateProcessor.hasExcpData()) {
	//// try {
	//// if (!namePlateProcessor.getExcepQuery().contains("values ON")) {
	////
	//// final String excpQuery = namePlateProcessor.getExcepQuery();
	////
	//// Future<Integer> future = expPool.submit(() -> {
	//// return schMtrTrckDao.bulkInsert(excpQuery);
	//// });
	////
	//// int excpCount = future.get(); // WAIT FOR EXECUTION
	//// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount +
	// " "
	//// + new Timestamp(new Date().getTime()));
	////
	//// namePlateProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	//// }
	//// } catch (Exception e) {
	//// e.printStackTrace();
	//// }
	//// }
	//
	// if (NamePlateProcessor.getInstance().hasData()) {
	// try {
	// if (!NamePlateProcessor.getInstance().getQuery().contains("values ON")) {
	//
	// final String mainQuery = NamePlateProcessor.getInstance().getQuery();
	//
	// try {
	// Collections.sort(meterNosForNamePlate);
	// moveOldNamePlateToHistory(meterNosForNamePlate);
	//
	// int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
	// totalCount_tl.set(totalCount_tl.get() + insertedCount);
	//
	// System.out.println(hes + " HES Load Count :: " + insertedCount + " "
	// + new Timestamp(System.currentTimeMillis()));
	//
	// NamePlateProcessor.getInstance().reset();
	// } catch (Exception ex) {
	// System.err.println(ex.getMessage());
	// throw new RuntimeException("Meter processing failed", ex);
	// }
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	// if (NamePlateProcessor.getInstance().hasExcpData()) {
	// try {
	// if (!NamePlateProcessor.getInstance().getExcepQuery().contains("values ON"))
	// {
	//
	// final String excpQuery = NamePlateProcessor.getInstance().getExcepQuery();
	//
	// Future<Integer> future = expPool.submit((Callable<Integer>) () -> {
	// return schMtrTrckDao.bulkInsert(excpQuery);
	// });
	//
	// int excpCount = future.get();
	// stats.setExceptionCount(stats.getExceptionCount() + excpCount);
	// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount + "
	// "
	// + new Timestamp(new Date().getTime()));
	//
	// NamePlateProcessor.getInstance().reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	// if (meterAlarmProcessor.hasData()) {
	// try {
	// if (!meterAlarmProcessor.getQuery().contains("values ON")) {
	//
	// final String mainQuery = meterAlarmProcessor.getQuery();
	// // System.out.println("Main Query:- "+blockLoadProcessor.getQuery());
	// Future<Integer> future = alarmPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(mainQuery);
	// });
	//
	// int insertedCount = future.get(); // WAIT FOR EXECUTION
	// totalCount += insertedCount;
	//
	// System.out.println("" + hes + " HES Load Count :: " + insertedCount + " "
	// + new Timestamp(new Date().getTime()));
	//
	// meterAlarmProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (meterAlarmProcessor.hasExcpData()) {
	// try {
	// if (!meterAlarmProcessor.getExcepQuery().contains("values ON")) {
	//
	// final String excpQuery = meterAlarmProcessor.getExcepQuery();
	//
	// Future<Integer> future = expPool.submit(() -> {
	// return schMtrTrckDao.bulkInsert(excpQuery);
	// });
	//
	// int excpCount = future.get(); // WAIT FOR EXECUTION
	// stats.setExceptionCount(stats.getExceptionCount() + excpCount);
	// System.out.println("" + hes + " HES Exception Load Count :: " + excpCount + "
	// "
	// + new Timestamp(new Date().getTime()));
	//
	// meterAlarmProcessor.reset(); // RESET AFTER SUCCESSFUL INSERT
	// }
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	//
	// if (isStringContainsSpecialCharacter || ispreVeeCharacter) {
	//// System.out.println(exceptionQuery.toString().substring(0,
	// exceptionQuery.length() - 1));
	// count = schMtrTrckDao.bulkInsert(
	// exceptionQuery.toString().substring(0, exceptionQuery.length() - 1) + " ON
	// CONFLICT DO NOTHING");
	// exceptionQuery.setLength(0);
	// ispreVeeCharacter = false;
	// System.out.println("----------Exception Count " + count + " " + new
	// Timestamp(new Date().getTime()));
	//
	// } else {
	//// System.out.println(query.toString().substring(0, query.length() - 1));
	// count = schMtrTrckDao.bulkInsert(query.toString().substring(0, query.length()
	// - 1));
	// query.setLength(0);// Reset
	// System.out.println("----------SchTracker Count " + count + " " + new
	// Timestamp(new Date().getTime()));
	//
	// }
	//
	// count = 0;
	// } finally {
	// // Log tracking data
	// kafkaTrackerService.logOffsetProcessing(topic, partition, offset, hes,
	// stats);
	// }
	// }

	public String zonedDateTimeConverter(String obisDate) {

		// Parse the string timestamp into a LocalDateTime object
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		LocalDateTime localDateTime = LocalDateTime.parse(obisDate, formatter);

		// Set the timezone of the LocalDateTime object to local time zone
		ZoneId localTimeZone = ZoneId.systemDefault();
		ZonedDateTime localZonedDateTime = localDateTime.atZone(localTimeZone);

		// Add time difference of 5 hours and 30 minutes
		ZonedDateTime gmt5_30ZonedDateTime = localZonedDateTime.plusHours(5).plusMinutes(30);

		// Convert to string timestamp in GMT+5:30 time zone
		String timestampGMT5_30 = gmt5_30ZonedDateTime.format(formatter);
		return timestampGMT5_30;

	}

	@Transactional
	public int moveOldNamePlateToHistory(List<String> meterNosForNamePlate) {
		if (meterNosForNamePlate == null || meterNosForNamePlate.isEmpty()) {
			return 0;
		}

		String meterList = meterNosForNamePlate.stream()
				.filter(m -> m != null && !m.isEmpty())
				.map(m -> "'" + m + "'")
				.collect(java.util.stream.Collectors.joining(","));

		if (meterList.isEmpty()) {
			return 0;
		}

		String sql = "WITH moved_rows AS ( " +
				"    DELETE FROM meter_data.name_plate " +
				"    WHERE meter_serial_number IN (" + meterList + ") " +
				"    RETURNING " +
				"        meter_serial_number, device_id, manufacturer_name, firmware_version, " +
				"        meter_type, meter_catagory, current_rating, hardware_version, " +
				"        year_of_manufacture, nflag, created_time, updated_time, node_id, " +
				"        ct_ratio, pt_ratio, server_time, hes_type " +
				") " +
				"INSERT INTO meter_data.name_plate_hst ( " +
				"    id, meter_serial_number, device_id, manufacturer_name, firmware_version, " +
				"    meter_type, meter_catagory, current_rating, hardware_version, " +
				"    year_of_manufacture, nflag, created_time, updated_time, node_id, " +
				"    ct_ratio, pt_ratio, server_time, hes_type " +
				") " +
				"SELECT nextval('meter_data.name_plate_id_seq'), * " +
				"FROM moved_rows";

		// System.out.println("HSTsql " + sql);
		return entityManager.createNativeQuery(sql).executeUpdate();
	}

	public static String resolveProfileCode(String profileCode, String topic) {
		if (profileCode != null && !profileCode.trim().isEmpty() && !"null".equalsIgnoreCase(profileCode.trim())
				&& !"Unknown Profile".equalsIgnoreCase(profileCode.trim())) {
			return profileCode.trim();
		}
		if (topic == null) {
			return "Unknown Profile";
		}
		String lowerTopic = topic.toLowerCase();
		if (lowerTopic.contains("blockload") || lowerTopic.contains("block_load") || lowerTopic.contains("load_survey")
				|| topic.equals(Subscriber.BCITS_TOPIC_LOAD) || topic.equals(Subscriber.JIO_TOPIC_LOAD)
				|| topic.equals(Subscriber.CYAN_TOPIC_LOAD)
				|| topic.equals(com.bcits.utils.Subscriber.KIMBAL_TOPIC_LOAD)) {
			return "1.0.99.1.0.255"; // Bulk Load
		}
		if (lowerTopic.contains("dailyload") || lowerTopic.contains("daily_load")
				|| topic.equals(Subscriber.BCITS_TOPIC_DAILYLOAD) || topic.equals(Subscriber.JIO_TOPIC_DAILYLOAD)
				|| topic.equals(Subscriber.CYAN_TOPIC_DAILYLOAD) || topic.equals(Subscriber.KIMBAL_TOPIC_DAILYLOAD)) {
			return "1.0.99.2.0.255"; // Daily Load
		}
		if (lowerTopic.contains("billing") || lowerTopic.contains("bill") || topic.equals(Subscriber.BCITS_TOPIC_BILL)
				|| topic.equals(Subscriber.JIO_TOPIC_BILL) || topic.equals(Subscriber.CYAN_TOPIC_BILL)
				|| topic.equals(Subscriber.KIMBAL_TOPIC_BILL)) {
			return "1.0.98.1.0.255"; // Billing
		}
		if (lowerTopic.contains("instant_push") || topic.equals(Subscriber.BCITS_TOPIC_INSTANT_PUSH)
				|| topic.equals(com.bcits.utils.Subscriber.JIO_TOPIC_INSTANT_PUSH)
				|| topic.equals(Subscriber.KIMBAL_TOPIC_INSTANT_PUSH) || topic.equals(Subscriber.CYAN_TOPIC_INSTANT)) {
			return "0.0.25.9.0.255"; // Instantaneous Push
		}
		if (lowerTopic.contains("instant_pull") || lowerTopic.contains("instant")
				|| topic.equals(Subscriber.BCITS_TOPIC_INSTANT) || topic.equals(Subscriber.JIO_TOPIC_INSTANT)
				|| topic.equals(Subscriber.KIMBAL_TOPIC_INSTANT)) {
			return "1.0.94.91.0.255"; // Instantaneous Pull
		}
		if (lowerTopic.contains("nameplate") || topic.equals(Subscriber.BCITS_TOPIC_NAMEPLATE)
				|| topic.equals(Subscriber.JIO_TOPIC_NAMEPLATE) || topic.equals(Subscriber.KIMBAL_TOPIC_NAMEPLATE)) {
			return "0.0.94.91.10.255"; // Name Plate
		}
		if (lowerTopic.contains("alarm") || topic.equals(Subscriber.BCITS_TOPIC_ALARM)
				|| topic.equals(Subscriber.JIO_TOPIC_ALARM) || topic.equals(Subscriber.CYAN_TOPIC_ALARM)
				|| topic.equals(Subscriber.KIMBAL_TOPIC_ALARM)) {
			return "0.4.25.9.0.255"; // Meter Alarm
		}
		if (lowerTopic.contains("event") || topic.equals(Subscriber.BCITS_TOPIC_EVENT)
				|| topic.equals(Subscriber.JIO_TOPIC_EVENT) || topic.equals(Subscriber.CYAN_TOPIC_EVENT)
				|| topic.equals(Subscriber.KIMBAL_TOPIC_EVENT)) {
			return "0.0.99.98.0.255"; // Default Event Log
		}
		return "Unknown Profile";
	}

	public void parseData(List<DeviceData> smm, String topic, String hes, int partition, long offset)
			throws ParseException, InterruptedException {

		if (smm == null || smm.isEmpty()) {
			return;
		}

		ProcessingStats stats = new ProcessingStats();
		stats.setTotalRecords(smm.size());

		BlockLoadProcessor bLoadProc = blockLoadProcessor.get();
		InstantaneousProcessor instProc = instantaneousProcessor.get();
		BillProcessor billProc = billProcessor.get();
		EventProcessor eventProc = eventProcessor.get();
		DailyLoadProcessor dLoadProc = dailyLoadProcessor.get();
		NamePlateProcessor nmProc = namePlateProcessor.get();
		MeterAlarmProcessor alarmProc = meterAlarmProcessor.get();

		bLoadProc.init();
		instProc.init();
		billProc.init();
		eventProc.init();
		dLoadProc.init();
		nmProc.init();
		alarmProc.init();

		StringBuilder query = new StringBuilder();
		query.append(
				"insert into meter_data.sch_meter_trackload (time_stamp ,seq_id, type, meter_number,apiname,hes_type) values ");
		StringBuilder exceptionQuery = new StringBuilder();
		exceptionQuery.append(
				"insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values ");
		Pattern pattern = Pattern.compile("[^a-zA-Z0-9_.-]");
		boolean isStringContainsSpecialCharacter = true;
		List<String> meterNosForNamePlate = new ArrayList<>();
		Gson gson = new Gson();

		try {
			for (DeviceData scMM : smm) {
				String smm1 = gson.toJson(scMM, new TypeToken<DeviceData>() {
				}.getType());
				long seq_id = scMM.getSeqId();
				String meter_number = scMM.getDeviceId();

				if (meter_number == null || meter_number.trim().isEmpty()) {
					isStringContainsSpecialCharacter = true;
					GenericExceptionTracker.logPayloadException(scMM.getSeqId(), "meter_number",
							"Device ID/Meter number is null or empty");
				} else {
					Matcher matcher = pattern.matcher(meter_number.trim());
					isStringContainsSpecialCharacter = matcher.find();
					if (isStringContainsSpecialCharacter) {
						GenericExceptionTracker.logPayloadException(scMM.getSeqId(), "meter_number",
								"Meter number contains special characters: " + meter_number);
					}
				}

				if (!isStringContainsSpecialCharacter) {

					String profileCode = resolveProfileCode(scMM.getProfileCode(), topic);
					boolean hasPreVee = (scMM.getPreVEE() != null && scMM.getPreVEE().length > 0);
					boolean isRecordValid = true;

					boolean isPreVeeRequired = !"0.4.25.9.0.255".equals(profileCode)
							|| "JIO".equalsIgnoreCase(hes)
							|| (scMM.getHesId() != null && "JIO".equalsIgnoreCase(scMM.getHesId()));

					if (isPreVeeRequired && !hasPreVee) {
						isRecordValid = false;

						// Determine type for stats reporting
						String tempType = "Unknown Profile";
						switch (profileCode) {
							case "1.0.99.1.0.255":
								tempType = "Bulk Load";
								break;
							case "0.0.25.9.0.255":
								tempType = "Instantaneous Push";
								break;
							case "1.0.94.91.0.255":
								tempType = "Instantaneous Pull";
								break;
							case "1.0.98.1.0.255":
								tempType = "Billing";
								break;
							case "0.0.99.98.0.255":
								tempType = "Voltage Event Log";
								break;
							case "0.0.99.98.1.255":
								tempType = "Current Event Log";
								break;
							case "0.0.99.98.2.255":
								tempType = "Power Event Log";
								break;
							case "0.0.99.98.3.255":
								tempType = "Transaction Event Log";
								break;
							case "0.0.99.98.4.255":
								tempType = "Other Event Log";
								break;
							case "0.0.99.98.5.255":
								tempType = "Non Roll Over Event Log";
								break;
							case "0.0.99.98.6.255":
								tempType = "Controled Event Log";
								break;
							case "1.0.99.2.0.255":
								tempType = "Daily Load";
								break;
							case "0.0.94.91.10.255":
								tempType = "Name Plate";
								break;
							case "0.4.25.9.0.255":
								tempType = "Meter Alarm";
								break;
						}

						stats.incrementFailure();
						stats.addType(tempType + " (preVEE missing)");

						exceptionQuery.append("(now(),'")
								.append(seq_id).append("','")
								.append(topic != null ? topic.replace("'", "''") : "").append("','")
								.append(meter_number != null ? meter_number.replace("'", "''") : "").append("','")
								.append(smm1.replace("'", "''")).append("','")
								.append("preVEE list is null or empty").append("','")
								.append(hes != null ? hes.replace("'", "''") : "").append("'),");
					}

					if (isRecordValid) {
						String type = "Unknown";
						switch (profileCode) {
							case "1.0.99.1.0.255":
								type = "Bulk Load";
								ResponseParser.type = type;
								bLoadProc.addData(scMM, hes);
								break;

							case "0.0.25.9.0.255":
								type = "Instantaneous Push";
								ResponseParser.type = type;
								instProc.addData(scMM, hes);
								break;

							case "1.0.94.91.0.255":
								type = "Instantaneous Pull";
								ResponseParser.type = type;
								instProc.addData(scMM, hes);
								break;

							case "1.0.98.1.0.255":
								type = "Billing";
								ResponseParser.type = type;
								billProc.addData(scMM, hes);
								break;

							case "0.0.99.98.0.255":
								type = "Voltage Event Log";
								ResponseParser.type = type;
								eventProc.addData(scMM, hes);
								break;

							case "0.0.99.98.1.255":
								type = "Current Event Log";
								ResponseParser.type = type;
								eventProc.addData(scMM, hes);
								break;

							case "0.0.99.98.2.255":
								type = "Power Event Log";
								ResponseParser.type = type;
								eventProc.addData(scMM, hes);
								break;

							case "0.0.99.98.3.255":
								type = "Transaction Event Log";
								ResponseParser.type = type;
								eventProc.addData(scMM, hes);
								break;

							case "0.0.99.98.4.255":
								type = "Other Event Log";
								ResponseParser.type = type;
								eventProc.addData(scMM, hes);
								break;

							case "0.0.99.98.5.255":
								type = "Non Roll Over Event Log";
								ResponseParser.type = type;
								eventProc.addData(scMM, hes);
								break;

							case "0.0.99.98.6.255":
								type = "Controled Event Log";
								ResponseParser.type = type;
								eventProc.addData(scMM, hes);
								break;

							case "1.0.99.2.0.255":
								type = "Daily Load";
								ResponseParser.type = type;
								dLoadProc.addData(scMM, hes);
								break;

							case "0.0.94.91.10.255":
								type = "Name Plate";
								ResponseParser.type = type;
								nmProc.addData(scMM, hes);
								meterNosForNamePlate.add(meter_number);
								break;

							case "0.4.25.9.0.255":
								type = "Meter Alarm";
								ResponseParser.type = type;
								alarmProc.addData(scMM, hes);
								break;

							default:
								type = "Unknown Profile";
								ResponseParser.type = type;
								System.out.println("Unrecognized profile code: " + profileCode);
								break;
						}

						String safeMeter = (meter_number != null ? meter_number.trim().replace("'", "''") : "");
						String safeTopic = (topic != null ? topic.trim().replace("'", "''") : "");
						String safeHes = (hes != null ? hes.trim().replace("'", "''") : "");
						String safeType = (type != null ? type.trim().replace("'", "''") : "Unknown");
						query.append("(now()," + seq_id + ", '" + safeType + "', '" + safeMeter + "','" + safeTopic
								+ "','" + safeHes
								+ "' ),");
						commTracker.add(meter_number, type);
						stats.incrementSuccess();
						stats.addType(type);
					}

				} else {
					List<String> exceptions = GenericExceptionTracker.getExceptions(String.valueOf(scMM.getSeqId()));
					String combinedExceptions = String.join(" | ", exceptions);
					exceptionQuery.append("(now(),'" + scMM.getSeqId() + "','"
							+ (topic != null ? topic.replace("'", "''") : "") + "','"
							+ (scMM.getDeviceId() != null ? scMM.getDeviceId().replace("'", "''") : "") + "','"
							+ smm1.replace("'", "''") + "','"
							+ combinedExceptions.replace("'", "''") + "','"
							+ (hes != null ? hes.replace("'", "''") : "") + "'),");
					stats.incrementFailure();
					stats.addType("Invalid Format");
				}
			}

			// Synchronous DB batch writes per partition worker thread (Direct, Non-blocking, No Lock Contention)
			if (bLoadProc.hasData()) {
				try {
					if (!bLoadProc.getQuery().contains("values ON")) {
						final String mainQuery = bLoadProc.getQuery();
						int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
						totalCount_tl.set(totalCount_tl.get() + insertedCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					bLoadProc.reset();
				}
			}

			if (bLoadProc.hasExcpData()) {
				try {
					if (!bLoadProc.getExcepQuery().contains("values ON")) {
						final String excpQuery = bLoadProc.getExcepQuery();
						int excpCount = schMtrTrckDao.bulkInsert(excpQuery);
						stats.addExceptionCount(excpCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					bLoadProc.reset();
				}
			}

			if (instProc.hasData()) {
				try {
					if (!instProc.getQuery().contains("values ON")) {
						final String mainQuery = instProc.getQuery();
						int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
						totalCount_tl.set(totalCount_tl.get() + insertedCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					instProc.reset();
				}
			}

			if (instProc.hasExcpData()) {
				try {
					if (!instProc.getExcepQuery().contains("values ON")) {
						final String excpQuery = instProc.getExcepQuery();
						int excpCount = schMtrTrckDao.bulkInsert(excpQuery);
						stats.addExceptionCount(excpCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					instProc.reset();
				}
			}

			if (billProc.hasData()) {
				try {
					if (!billProc.getQuery().contains("values ON")) {
						final String mainQuery = billProc.getQuery();
						int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
						totalCount_tl.set(totalCount_tl.get() + insertedCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					billProc.reset();
				}
			}

			if (billProc.hasExcpData()) {
				try {
					if (!billProc.getExcepQuery().contains("values ON")) {
						final String excpQuery = billProc.getExcepQuery();
						int excpCount = schMtrTrckDao.bulkInsert(excpQuery);
						stats.addExceptionCount(excpCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					billProc.reset();
				}
			}

			if (eventProc.hasData()) {
				try {
					if (!eventProc.getQuery().contains("values ON")) {
						final String mainQuery = eventProc.getQuery();
						int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
						totalCount_tl.set(totalCount_tl.get() + insertedCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					eventProc.reset();
				}
			}

			if (eventProc.hasExcpData()) {
				try {
					if (!eventProc.getExcepQuery().contains("values ON")) {
						final String excpQuery = eventProc.getExcepQuery();
						int excpCount = schMtrTrckDao.bulkInsert(excpQuery);
						stats.addExceptionCount(excpCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					eventProc.reset();
				}
			}

			if (dLoadProc.hasData()) {
				try {
					if (!dLoadProc.getQuery().contains("values ON")) {
						final String mainQuery = dLoadProc.getQuery();
						int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
						totalCount_tl.set(totalCount_tl.get() + insertedCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					dLoadProc.reset();
				}
			}

			if (dLoadProc.hasExcpData()) {
				try {
					if (!dLoadProc.getExcepQuery().contains("values ON")) {
						final String excpQuery = dLoadProc.getExcepQuery();
						int excpCount = schMtrTrckDao.bulkInsert(excpQuery);
						stats.addExceptionCount(excpCount);
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					dLoadProc.reset();
				}
			}

			if (nmProc.hasData()) {
				try {
					if (!nmProc.getQuery().contains("values ON")) {
						final String mainQuery = nmProc.getQuery();
						try {
							Collections.sort(meterNosForNamePlate);
							moveOldNamePlateToHistory(meterNosForNamePlate);
						} catch (Exception ex) {
							log.warn("NamePlate move history warning: {}", ex.getMessage());
						}
						int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
						totalCount_tl.set(totalCount_tl.get() + insertedCount);
					}
				} catch (Exception e) {
					log.error("Error executing NamePlate query: {}", e.getMessage(), e);
				} finally {
					nmProc.reset();
				}
			}

			if (nmProc.hasExcpData()) {
				try {
					if (!nmProc.getExcepQuery().contains("values ON")) {
						final String excpQuery = nmProc.getExcepQuery();
						int excpCount = schMtrTrckDao.bulkInsert(excpQuery);
						stats.addExceptionCount(excpCount);
					}
				} catch (Exception e) {
					log.error("Error executing NamePlate exception query: {}", e.getMessage(), e);
				} finally {
					nmProc.reset();
				}
			}

			if (alarmProc.hasData()) {
				try {
					if (!alarmProc.getQuery().contains("values ON")) {
						final String mainQuery = alarmProc.getQuery();
						int insertedCount = schMtrTrckDao.bulkInsert(mainQuery);
						totalCount_tl.set(totalCount_tl.get() + insertedCount);
					}
				} catch (Exception e) {
					log.error("Error executing Alarm query: {}", e.getMessage(), e);
				} finally {
					alarmProc.reset();
				}
			}

			if (alarmProc.hasExcpData()) {
				try {
					if (!alarmProc.getExcepQuery().contains("values ON")) {
						final String excpQuery = alarmProc.getExcepQuery();
						int excpCount = schMtrTrckDao.bulkInsert(excpQuery);
						stats.addExceptionCount(excpCount);
					}
				} catch (Exception e) {
					log.error("Error executing Alarm exception query: {}", e.getMessage(), e);
				} finally {
					alarmProc.reset();
				}
			}

			if (exceptionQuery
					.length() > "insert into meter_data.exception_seq_tracker (time_stamp, seq_id, type,meter_number,data,exp_remarks,hes_type) values "
							.length()) {
				schMtrTrckDao.bulkInsert(exceptionQuery.toString().substring(0, exceptionQuery.length() - 1)
						+ " ON CONFLICT DO NOTHING");
			}

			if (query
					.length() > "insert into meter_data.sch_meter_trackload (time_stamp ,seq_id, type, meter_number,apiname,hes_type) values "
							.length()) {
				schMtrTrckDao
						.bulkInsert(query.toString().substring(0, query.length() - 1) + " ON CONFLICT DO NOTHING");
			}

		} finally {
			// Log tracking data safely after all inserts are complete
			try {
				kafkaTrackerService.logOffsetProcessing(topic, partition, offset, hes, stats);
			} catch (Throwable trackerEx) {
				log.warn("[KAFKA-TRACKER] Error logging offset tracking: {}", trackerEx.getMessage());
			}
		}
	}

}
