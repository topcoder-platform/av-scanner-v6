import { createLogger, format, transports, type Logger } from "winston";

/**
 * Creates the process logger used by the scanner application.
 *
 * @param level - Winston log level selected from LOG_LEVEL.
 * @returns A JSON console logger suitable for ECS log aggregation.
 */
export function createAppLogger(level: string): Logger {
  return createLogger({
    level,
    format: format.combine(format.timestamp(), format.json()),
    transports: [new transports.Console()],
  });
}
