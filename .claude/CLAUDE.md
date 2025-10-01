# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Hummingbot is an open-source algorithmic trading framework supporting 140+ exchanges (CEX/DEX) with automated trading strategies. The codebase focuses on connector development for new exchanges and strategy optimization/creation.

## Development Environment

**Required Environment**: Always use `conda activate hummingbot` before any development work.

### Common Commands

```bash
# Initial setup
./install                    # Setup conda environment and dependencies
./install --dydx            # Setup with dYdX-specific dependencies

# Build and compilation
./compile                   # Compile Cython extensions
make build                  # Alternative build command

# Running Hummingbot
./start                     # Start Hummingbot CLI
./start -p password -f strategy.yml -c config.yml  # Start with specific params
make run-v2                 # Run V2 strategies

# Testing
make test                   # Run full test suite with coverage
pytest test/path/to/test    # Run specific tests
pytest --timeout=3         # Run tests with timeout (recommended)

# Coverage and linting
make run_coverage          # Generate coverage reports
make report_coverage       # View coverage reports
```

## Architecture Overview

### Core Directory Structure

- `hummingbot/core/`: Core framework functionality (data types, events, utilities)
- `hummingbot/connector/`: Exchange integrations
  - `exchange/`: Spot trading connectors (CEX)
  - `derivative/`: Perpetual futures connectors
  - `gateway/`: DEX connectors via Gateway service
- `hummingbot/strategy/`: V1 trading strategies
- `hummingbot/strategy_v2/`: V2 strategy framework
- `hummingbot/client/`: CLI interface and configuration
- `test/`: Comprehensive test suite mirroring source structure

### Connector Architecture

**Spot Connectors**: `hummingbot/connector/exchange/{connector_name}/`
**Perpetual Connectors**: `hummingbot/connector/derivative/{connector_name}/`

Reference implementations:
- **CEX**: Binance (spot & derivatives) - most mature implementation
- **DEX**: Hyperliquid - best on-chain exchange reference

Each connector implements standardized REST/WebSocket APIs enabling strategy deployment across exchanges with minimal changes.

## Connector Development Workflow

### Critical Rules from Cursor Guidelines

1. **NEVER** modify high-level design without confirmation
2. **ALWAYS** track planning at `./local_dev/plans/{exchange}`
3. **ALWAYS** stop and test before marking milestones complete
4. **NEVER** make API assumptions - verify with:
   - `./local_dev/docs/{exchange}/openapi.json` for API specs
   - `./local_dev/api-examples/{exchange}/` for API examples
5. For derivatives: leverage spot trading components (especially auth)

### Development Process

1. **API Analysis**: Check `local_dev/docs/api_checklist.csv` compatibility first
2. **Test Planning**: Design test strategy for REST and WebSocket before coding
3. **TDD Implementation**: Write tests first, implement features second
4. **Milestone Testing**: Stop and test before moving to next phase

### Testing Guidelines

- Run `conda activate hummingbot` before triggering tests
- Use `pytest --timeout=3` for timeout settings
- NEVER shortcut source code to make tests pass
- Test coverage is tracked and required

## Exchange Integration Types

**Market Types**:
- **CLOB Spot**: Central limit order book spot markets
- **CLOB Perp**: Perpetual futures on CLOB exchanges
- **AMM**: Automated market maker DEX spot markets

**Exchange Types**:
- **CEX**: Centralized exchanges (API key authentication)
- **DEX**: Decentralized exchanges (wallet key authentication)

## Build System

- **Language**: Python 3.10+ with Cython extensions for performance
- **Dependencies**: Managed via Conda (`setup/environment.yml`)
- **Build Process**: Uses `setup.py` with Cython compilation
- **Package Management**: Conda for system deps, pip for Python packages

## Key Files

- `setup.py`: Build configuration and dependency management
- `Makefile`: Common development tasks and test execution
- `pyproject.toml`: Code formatting and test configuration (Black, isort, pytest)
- `./local_dev/docs/`: API documentation and checklists for connector development
