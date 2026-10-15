package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.AppAdapterIoMsgsDao;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.entity.AppAdapterIoMsgs;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.Dir;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.utils.AdapterEntityFactory;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProducerStatusService {

    private final AppAdapterTransDao transDao;
    private final AppAdapterIoMsgsDao ioMsgsDao;
    private final AdapterEntityFactory adapterEntityFactory;


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveInitialTrans(AppAdapterTrans trans) {
        trans.setStatus(FocStatus.CONSUMER_WAIT)
                .setRespCode(null)
                .setRespDesc("Waiting for Kafka response");

        transDao.save(trans);
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveSuccessLog(AppAdapterTrans trans, String json, Long nodeId) {
        AppAdapterIoMsgs ioMsg = adapterEntityFactory.createIoMsg(trans.getId(), Dir.OUT, json, nodeId);
        ioMsgsDao.save(ioMsg);
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsError(AppAdapterTrans trans, String errorMsg) {
        trans.setStatus(FocStatus.ERROR)
                .setRespCode("500")
                .setRespDesc(errorMsg);

        transDao.updateStatus(trans);
    }
}