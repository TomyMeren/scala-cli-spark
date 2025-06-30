# PlaySpark - Scala & Apache Spark Playground

A comprehensive playground for experimenting with Apache Spark and related libraries using scala-cli.

## Overview

This project showcases different approaches to working with Apache Spark in Scala:
- **Spark Vanilla**: Traditional Spark DataFrame API
- **Doric**: Type-safe column expressions with compile-time error checking
- **Frameless**: Type-safe Dataset API
- **GraphFrames**: Graph analytics with Spark

## Getting Started

### Prerequisites
- Java 11+ (configured to use Java 11 specifically)
- Scala CLI installed
- Optional: SBT for more complex builds

### Running Examples

```bash
# Run a simple Spark example
scala-cli run . scripts/SparkVanilla/SparkHelloWord.sc

# Run Doric type-safe examples
scala-cli run . scripts/Doric/Case1.sc

# Run Frameless typed dataset examples  
scala-cli run . scripts/Frameless/EmptyDf.sc

# Run GraphFrames examples
scala-cli run . scripts/GraphFrame/GraphFrameQuickStart.sc
```

## Project Structure

```
├── project.scala           # Dependencies and build configuration
├── resources/              # Configuration files
│   └── log4j2.properties  # Logging configuration
└── scripts/                # Example scripts organized by library
    ├── Doric/             # Type-safe column expressions
    ├── Frameless/         # Type-safe Datasets
    ├── GraphFrame/        # Graph analytics
    └── SparkVanilla/      # Traditional Spark DataFrame API
```

## Libraries Used

- **Apache Spark 3.5.6**: Core distributed computing engine
- **Doric 0.0.8**: Type-safe column expressions for Spark
- **Frameless 0.16.0**: Type-safe functional programming with Spark
- **GraphFrames 0.8.4**: Graph analytics on top of DataFrames

## Examples Overview

### Doric Examples
- **Case1-7**: Progressive examples showing type-safe column operations
- **ExtraExamples/**: Advanced features like error reporting, validations, and modularity

### Frameless Examples
- Type-safe Dataset operations with compile-time guarantees

### GraphFrames Examples
- Graph algorithms and analytics using DataFrame API

## Contributing

Feel free to add more examples or improve existing ones. Each script should be self-contained and demonstrate specific concepts.# Playgraound of scala-cli and Spark