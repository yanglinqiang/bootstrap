#!/usr/bin/env bash

ps -ef | grep java  | grep 'com.ylq.framework.Bootstrap' | grep -v 'grep' |  awk '{print $2}' | xargs  kill