package ru.itone.illya4gurenko.entity.enums;

/**
 * тип регламента обработки записи по времени.
 */
public enum ProcType {
    /** обработка сразу при поступлении */
    IMMEDIATE,

    /** отложенная обработка к определенной дате и времени */
    INTIME
}