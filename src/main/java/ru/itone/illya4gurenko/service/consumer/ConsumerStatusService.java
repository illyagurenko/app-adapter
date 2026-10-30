package ru.itone.illya4gurenko.service.consumer;

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

/**
 * сервис управления промежуточными и финальными статусами транзакций при обработке входящих ответов.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerStatusService {

    private final AppAdapterTransDao transDao;
    private final AppAdapterIoMsgsDao ioMsgsDao;
    private final AdapterEntityFactory adapterEntityFactory;

    /**
     * переводит транзакцию в статус CONSUMER_IN_PROCESS и сохраняет входящее сырое сообщение в таблицу аудита.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void startProcessing(AppAdapterTrans trans, String json, Long nodeId) {
        trans.setStatus(FocStatus.CONSUMER_IN_PROCESS)
                .setRespDesc("consumer processing response");

        transDao.updateStatus(trans);

        AppAdapterIoMsgs ioMsg = adapterEntityFactory.createIoMsg(trans.getId(), Dir.IN, json, nodeId);
        ioMsgsDao.save(ioMsg);

        log.info("transaction requestId={} moved to {} and IO msg saved",
                trans.getRequestId(),
                FocStatus.CONSUMER_IN_PROCESS
        );
    }

    /**
     * фиксирует финальный статус (SUCCESS/ERROR), код и описание завершения обработки транзакции.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateFinalStatus(AppAdapterTrans trans, FocStatus status, String respCode, String respDesc) {
        trans.setStatus(status)
                .setRespCode(respCode)
                .setRespDesc(respDesc);

        transDao.updateStatus(trans);
        log.info("transaction requestId={} finalized with status={}", trans.getRequestId(), status);
    }
}