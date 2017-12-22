package com.lucanet.packratcollector.aspects;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class LogExecutionAspect {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final Logger logger;

  // ============================  Constructors  ===========================79
  public LogExecutionAspect() {
    logger = LoggerFactory.getLogger(LogExecutionAspect.class);
  }

  // ============================ Public Methods ===========================79
  @Before("@annotation(LogExecution)")
  public void logBefore(JoinPoint joinPoint) {
    logger.trace("Entering: {}:{}", joinPoint.getTarget().getClass().getName(), joinPoint.getSignature().getName());
  }

  @After("@annotation(LogExecution)")
  public void logAfter(JoinPoint joinPoint) {
    logger.trace("Exiting: {}:{}", joinPoint.getTarget().getClass().getName(), joinPoint.getSignature().getName());
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
