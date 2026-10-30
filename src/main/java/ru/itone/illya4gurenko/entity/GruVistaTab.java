package ru.itone.illya4gurenko.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.itone.illya4gurenko.entity.enums.FocStatus;
import ru.itone.illya4gurenko.entity.enums.ProcType;
import ru.itone.illya4gurenko.entity.enums.Type;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * сущность для взаимодействия с таблицей GRU_VISTA_TAB (содержит исходные данные для обработки).
 */
@Data
@Accessors(chain = true)
public class GruVistaTab {

    private Long id;

    private String systemAccount;

    private String currency;

    private BigDecimal xalfa;

    private Type operation;

    private LocalDateTime timeStamp;

    private Long pomId;

    private Long uterrario;

    private BigDecimal oldTBal;

    private BigDecimal newTBal;

    private String addInfo;

    private Long fileId;

    private FocStatus focStatus;

    private LocalDateTime focTS;

    private ProcType focType;

    private LocalDateTime focStatusTs;

}